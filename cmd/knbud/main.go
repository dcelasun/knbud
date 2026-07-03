package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dcelasun/knbud/internal/config"
	"github.com/dcelasun/knbud/internal/discovery"
	"github.com/dcelasun/knbud/internal/executor"
	"github.com/dcelasun/knbud/internal/graph"
	"github.com/dcelasun/knbud/internal/kube"
	"github.com/dcelasun/knbud/internal/model"
	"github.com/dcelasun/knbud/internal/output"
	"github.com/dcelasun/knbud/internal/planner"
	"github.com/urfave/cli/v3"
)

type options struct {
	config            string
	kubeconfig        string
	context           string
	output            string
	parallelism       int
	timeout           time.Duration
	yes               bool
	acceptSuggestions bool
	ignoreSuggestions bool
	dryRun            bool
}

func main() {
	command := newCommand(os.Stdin, os.Stdout, os.Stderr)
	if err := command.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func newCommand(stdin io.Reader, stdout, stderr io.Writer) *cli.Command {
	command := &cli.Command{
		Name:      "knbud",
		Usage:     "Manage NFS-dependent Kubernetes workloads",
		Reader:    stdin,
		Writer:    stdout,
		ErrWriter: stderr,
		Commands: []*cli.Command{
			{
				Name:   "discover",
				Usage:  "Discover NFS users and dependency evidence",
				Flags:  discoverFlags(),
				Action: discoverAction(stdin, stdout),
			},
			{
				Name:  "plan",
				Usage: "Plan and apply maintenance ordering",
				Commands: []*cli.Command{
					{Name: "down", Usage: "Stop NFS-dependent workloads", Flags: planFlags(), Action: planExecuteAction(planner.Down, stdin, stdout)},
					{Name: "up", Usage: "Restore NFS-dependent workloads", Flags: planFlags(), Action: planExecuteAction(planner.Up, stdin, stdout)},
				},
			},
			{
				Name:  "config",
				Usage: "Update configuration without editing YAML",
				Commands: []*cli.Command{
					{
						Name: "dependency", Usage: "Manage explicit dependencies", Commands: []*cli.Command{{
							Name: "add", Usage: "Add a dependency", Flags: append(connectionFlags(),
								&cli.StringSliceFlag{Name: "consumer", Usage: "Consumer kind/namespace/name"},
								&cli.StringSliceFlag{Name: "provider", Usage: "Provider kind/namespace/name"},
							), Action: configDependencyAddAction(stdin, stdout),
						}},
					},
					{
						Name: "include", Usage: "Include workloads explicitly", Flags: append(connectionFlags(),
							&cli.StringSliceFlag{Name: "resource", Usage: "Resource kind/namespace/name"},
						), Action: configIncludeAction(stdin, stdout),
					},
					{
						Name: "gitops", Usage: "Manage GitOps integration", Commands: []*cli.Command{{
							Name: "enable", Usage: "Enable a GitOps provider", Flags: append(connectionFlags(), &cli.StringFlag{Name: "mode", Value: "auto"}),
							Action: configGitOpsEnableAction(stdout),
						}},
					},
				},
			},
		},
	}
	return command
}

func configDependencyAddAction(stdin io.Reader, stdout io.Writer) cli.ActionFunc {
	return func(ctx context.Context, command *cli.Command) error {
		opts, err := connectionOptions(command)
		if err != nil {
			return err
		}
		cfg, snapshot, result, err := loadConfigurationContext(ctx, opts)
		if err != nil {
			return err
		}
		consumerValues := command.StringSlice("consumer")
		providerValues := command.StringSlice("provider")
		interactive := len(consumerValues) == 0 && len(providerValues) == 0
		var reader *bufio.Reader
		var consumers, providers []model.Ref
		if interactive {
			if !interactiveTerminal(stdin) {
				return fmt.Errorf("consumer and provider flags are required without an interactive terminal")
			}
			reader = bufio.NewReader(stdin)
			consumers, err = selectWorkloads(reader, stdout, "consumer", result.Inventory.Workloads)
			if err != nil {
				return err
			}
			providers, err = selectWorkloads(reader, stdout, "provider", result.Inventory.Workloads)
			if err != nil {
				return err
			}
		} else {
			if len(consumerValues) == 0 || len(providerValues) == 0 {
				return fmt.Errorf("both --consumer and --provider are required")
			}
			consumers, err = parseRefs(consumerValues)
			if err != nil {
				return err
			}
			providers, err = parseRefs(providerValues)
			if err != nil {
				return err
			}
			if err := requireKnownRefs(result, append(append([]model.Ref{}, consumers...), providers...)); err != nil {
				return err
			}
		}
		cfg.AcceptCustomDependency(consumers, providers)
		plan, err := validateConfiguration(snapshot, cfg)
		if err != nil {
			return err
		}
		printPlanSummary(stdout, plan)
		if interactive {
			confirmed, err := askYesNo(reader, stdout, "Write this configuration?", false)
			if err != nil {
				return err
			}
			if !confirmed {
				fmt.Fprintln(stdout, "Configuration unchanged.")
				return nil
			}
		}
		if err := config.Write(opts.config, cfg); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "Updated %s.\n", opts.config)
		return nil
	}
}

func configIncludeAction(stdin io.Reader, stdout io.Writer) cli.ActionFunc {
	return func(ctx context.Context, command *cli.Command) error {
		opts, err := connectionOptions(command)
		if err != nil {
			return err
		}
		cfg, snapshot, result, err := loadConfigurationContext(ctx, opts)
		if err != nil {
			return err
		}
		values := command.StringSlice("resource")
		interactive := len(values) == 0
		var reader *bufio.Reader
		var refs []model.Ref
		if interactive {
			if !interactiveTerminal(stdin) {
				return fmt.Errorf("resource flags are required without an interactive terminal")
			}
			reader = bufio.NewReader(stdin)
			refs, err = selectWorkloads(reader, stdout, "include", result.Inventory.Workloads)
		} else {
			refs, err = parseRefs(values)
		}
		if err != nil {
			return err
		}
		if err := requireKnownRefs(result, refs); err != nil {
			return err
		}
		cfg.IncludeResources(refs)
		plan, err := validateConfiguration(snapshot, cfg)
		if err != nil {
			return err
		}
		printPlanSummary(stdout, plan)
		if interactive {
			confirmed, err := askYesNo(reader, stdout, "Write this configuration?", false)
			if err != nil {
				return err
			}
			if !confirmed {
				fmt.Fprintln(stdout, "Configuration unchanged.")
				return nil
			}
		}
		if err := config.Write(opts.config, cfg); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "Updated %s.\n", opts.config)
		return nil
	}
}

func configGitOpsEnableAction(stdout io.Writer) cli.ActionFunc {
	return func(ctx context.Context, command *cli.Command) error {
		if command.NArg() != 1 || command.Args().First() != model.ProviderFlux {
			return fmt.Errorf("provider must be flux")
		}
		opts := options{config: command.String("config"), kubeconfig: command.String("kubeconfig"), context: command.String("context")}
		mode := command.String("mode")
		if mode != "auto" {
			return fmt.Errorf("Flux mode must be auto")
		}
		cfg, snapshot, _, err := loadConfigurationContext(ctx, opts)
		if err != nil {
			return err
		}
		cfg.GitOps.Flux = config.GitOpsProvider{Enabled: true, Mode: mode}
		plan, err := validateConfiguration(snapshot, cfg)
		if err != nil {
			return err
		}
		printPlanSummary(stdout, plan)
		if err := config.Write(opts.config, cfg); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "Updated %s.\n", opts.config)
		return nil
	}
}

func loadConfigurationContext(ctx context.Context, opts options) (*config.Config, *kube.Snapshot, *discovery.Result, error) {
	cfg, err := config.Load(opts.config)
	if err != nil {
		return nil, nil, nil, err
	}
	snapshot, _, err := loadSnapshot(ctx, opts)
	if err != nil {
		return nil, nil, nil, err
	}
	result, err := discovery.Build(snapshot, cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	return cfg, snapshot, result, nil
}

func parseRefs(values []string) ([]model.Ref, error) {
	var refs []model.Ref
	for _, value := range values {
		parts := strings.Split(value, "/")
		if len(parts) != 3 {
			return nil, fmt.Errorf("resource %q must use kind/namespace/name", value)
		}
		kind, err := model.ParseKind(parts[0])
		if err != nil {
			return nil, err
		}
		if parts[1] == "" || parts[2] == "" {
			return nil, fmt.Errorf("resource %q must include namespace and name", value)
		}
		refs = append(refs, model.Ref{Kind: kind, Namespace: parts[1], Name: parts[2]})
	}
	return refs, nil
}

func requireKnownRefs(result *discovery.Result, refs []model.Ref) error {
	for _, ref := range refs {
		if result.Inventory.Workloads[ref.ID()] == nil {
			return fmt.Errorf("workload %s was not found", ref.ID())
		}
	}
	return nil
}

func commonFlags(mutable bool) []cli.Flag {
	flags := append(connectionFlags(),
		&cli.StringFlag{Name: "output", Value: "human", Usage: "Output format: human or json"},
		&cli.IntFlag{Name: "parallelism", Value: 8, Usage: "Maximum concurrent operations"},
		&cli.DurationFlag{Name: "timeout", Value: 5 * time.Minute, Usage: "Timeout per workload"},
	)
	if mutable {
		flags = append(flags, &cli.BoolFlag{Name: "yes", Usage: "Skip confirmation"})
	}
	return flags
}

func planFlags() []cli.Flag {
	return append(commonFlags(true), &cli.BoolFlag{Name: "dry-run", Usage: "Render the plan without applying it"})
}

func connectionFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "config", Value: "knbud.yaml", Usage: "Configuration file"},
		&cli.StringFlag{Name: "kubeconfig", Usage: "Kubeconfig file"},
		&cli.StringFlag{Name: "context", Usage: "Kubeconfig context"},
	}
}

func discoverFlags() []cli.Flag {
	return append(commonFlags(false),
		&cli.BoolFlag{Name: "dry-run", Usage: "Print all discoveries without writing configuration"},
		&cli.BoolFlag{Name: "accept-suggestions", Usage: "Accept all dependency candidates"},
		&cli.BoolFlag{Name: "ignore-suggestions", Usage: "Ignore all dependency candidates"},
	)
}

func discoverAction(stdin io.Reader, stdout io.Writer) cli.ActionFunc {
	return func(ctx context.Context, command *cli.Command) error {
		opts, err := commandOptions(command)
		if err != nil {
			return err
		}
		result, _, cfg, snapshot, inferred, err := loadDiscovery(ctx, opts, !command.IsSet("config"))
		if err != nil {
			return err
		}
		candidates := model.DependencyCandidates(result.Inventory.Suggestions)
		if opts.dryRun || opts.output == "json" {
			return output.Discovery(stdout, result, opts.output)
		}
		accepted := make(map[int]bool)
		interactive := !opts.acceptSuggestions && !opts.ignoreSuggestions
		var buffered *bufio.Reader
		if interactive {
			buffered = bufio.NewReader(stdin)
		}
		switch {
		case opts.acceptSuggestions:
			for index := range candidates {
				accepted[index] = true
			}
		case opts.ignoreSuggestions:
		case !interactiveTerminal(stdin):
			return fmt.Errorf("interactive configuration requires a terminal; use --accept-suggestions or --ignore-suggestions")
		default:
			accepted, err = selectCandidates(buffered, stdout, candidates)
			if err != nil {
				return err
			}
		}
		acceptedEdges := make(map[string]bool)
		for index, candidate := range candidates {
			if accepted[index] {
				cfg.Accept(candidate)
				acceptedEdges[model.Edge{Consumer: candidate.Consumer, Provider: candidate.Provider}.ID()] = true
			}
		}
		inferredEdges := persistInferredEdges(cfg, result)
		if interactive {
			if err := configureInteractively(buffered, stdout, cfg, result); err != nil {
				return err
			}
		}
		if err := resolveCycles(buffered, stdout, snapshot, cfg, acceptedEdges); err != nil {
			return err
		}
		plan, err := validateConfiguration(snapshot, cfg)
		if err != nil {
			return err
		}
		printPlanSummary(stdout, plan)
		if interactive {
			confirmed, err := askYesNo(buffered, stdout, "Write this configuration?", false)
			if err != nil {
				return err
			}
			if !confirmed {
				fmt.Fprintln(stdout, "Configuration unchanged.")
				return nil
			}
		}
		fmt.Fprintf(stdout, "Recording %d accepted and %d inferred dependencies.\n", len(accepted), inferredEdges)
		if inferred {
			if err := config.Create(opts.config, cfg); err != nil {
				return err
			}
		} else if err := config.Write(opts.config, cfg); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "Updated %s.\n", opts.config)
		return nil
	}
}

// persistInferredEdges records the automatically inferred service-reference
// dependencies into the configuration so that plans no longer depend on live
// inference. It returns the number of edges written.
func persistInferredEdges(cfg *config.Config, result *discovery.Result) int {
	count := 0
	for _, edge := range result.Inventory.Edges {
		if edge.Type != model.EvidenceService {
			continue
		}
		cfg.AcceptDependency([]model.Ref{edge.Consumer}, []model.Ref{edge.Provider})
		count++
	}
	return count
}

func selectCandidates(reader *bufio.Reader, writer io.Writer, candidates []model.DependencyCandidate) (map[int]bool, error) {
	selected := make(map[int]bool)
	if len(candidates) == 0 {
		return selected, nil
	}
	fmt.Fprintln(writer, "\nSelect dependency candidates to accept. Unselected candidates will be ignored:")
	for index, candidate := range candidates {
		fmt.Fprintf(writer, "  %d. %s -> %s\n     %s: %s\n", index+1, candidate.Consumer.ID(), candidate.Provider.ID(), candidate.Reason, strings.Join(candidate.Evidence, ", "))
	}
	fmt.Fprint(writer, "\nEnter comma-separated numbers, all, or none: ")
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	accepted, err := parseSelection(line, len(candidates))
	if err != nil {
		return nil, err
	}
	return accepted, nil
}

func configureInteractively(reader *bufio.Reader, writer io.Writer, cfg *config.Config, result *discovery.Result) error {
	if !cfg.GitOps.Flux.Enabled && fluxDetected(result) {
		enabled, err := askYesNo(reader, writer, "Flux manages discovered workloads. Suspend and resume Flux automatically during maintenance?", true)
		if err != nil {
			return err
		}
		if enabled {
			cfg.GitOps.Flux = config.GitOpsProvider{Enabled: true, Mode: "auto"}
		}
	}
	firstDependency := true
	for {
		question := "Add another dependency?"
		if firstDependency {
			question = "Add a dependency that could not be discovered automatically (for example, one stored in a Secret)?"
		}
		add, err := askYesNo(reader, writer, question, false)
		if err != nil {
			return err
		}
		if !add {
			break
		}
		consumers, err := selectWorkloads(reader, writer, "consumer", result.Inventory.Workloads)
		if err != nil {
			return err
		}
		providers, err := selectWorkloads(reader, writer, "provider", result.Inventory.Workloads)
		if err != nil {
			return err
		}
		cfg.AcceptCustomDependency(consumers, providers)
		firstDependency = false
	}
	firstInclude := true
	for {
		question := "Include another workload?"
		if firstInclude {
			question = "Include a workload that does not directly use NFS?"
		}
		add, err := askYesNo(reader, writer, question, false)
		if err != nil {
			return err
		}
		if !add {
			break
		}
		refs, err := selectWorkloads(reader, writer, "include", result.Inventory.Workloads)
		if err != nil {
			return err
		}
		cfg.IncludeResources(refs)
		firstInclude = false
	}
	return nil
}

func selectWorkloads(reader *bufio.Reader, writer io.Writer, role string, workloads map[string]*model.Workload) ([]model.Ref, error) {
	var matches []model.Ref
	for {
		fmt.Fprintf(writer, "Search for the %s workload: ", role)
		line, err := reader.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		filter := strings.ToLower(strings.TrimSpace(line))
		if filter == "" {
			fmt.Fprintln(writer, "Enter part of its name or namespace.")
			continue
		}
		matches = nil
		for _, workload := range model.SortedWorkloads(workloads) {
			if strings.Contains(strings.ToLower(workload.Ref.ID()), filter) {
				matches = append(matches, workload.Ref)
			}
		}
		if len(matches) == 0 {
			fmt.Fprintf(writer, "No workloads match %q. Try a different search.\n", filter)
			continue
		}
		if len(matches) > 20 {
			fmt.Fprintf(writer, "%d workloads match %q. Use a more specific search.\n", len(matches), filter)
			continue
		}
		break
	}
	for index, ref := range matches {
		fmt.Fprintf(writer, "  %d. %s\n", index+1, ref.ID())
	}
	fmt.Fprintf(writer, "Select %s workloads (comma-separated numbers): ", role)
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	selected, err := parseSelection(line, len(matches))
	if err != nil {
		return nil, err
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("at least one %s workload must be selected", role)
	}
	refs := make([]model.Ref, 0, len(selected))
	for index := range selected {
		refs = append(refs, matches[index])
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].ID() < refs[j].ID() })
	return refs, nil
}

func askYesNo(reader *bufio.Reader, writer io.Writer, question string, defaultValue bool) (bool, error) {
	suffix := "[y/N]"
	if defaultValue {
		suffix = "[Y/n]"
	}
	fmt.Fprintf(writer, "%s %s: ", question, suffix)
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	if answer == "" {
		return defaultValue, nil
	}
	return answer == "y" || answer == "yes", nil
}

func fluxDetected(result *discovery.Result) bool {
	for _, resource := range result.Inventory.GitOpsResources {
		if resource.Ref.Provider == model.ProviderFlux {
			return true
		}
	}
	return false
}

// resolveCycles removes cycles created by inferred or accepted dependencies so
// that discover never fails on them: it drops the single accepted candidate in a
// cycle automatically, and prompts (or picks deterministically without a
// terminal) when the choice is ambiguous. A cycle that involves a custom
// dependency is reported as a hard error, since the operator must resolve it.
func resolveCycles(reader *bufio.Reader, writer io.Writer, snapshot *kube.Snapshot, cfg *config.Config, acceptedEdges map[string]bool) error {
	for {
		result, err := discovery.BuildPlan(snapshot, cfg)
		if err != nil {
			return err
		}
		selected := graph.ConsumerClosureExcept(result.Included, result.Inventory.Edges, result.Excluded)
		cycle := graph.FindCycle(selected, result.Inventory.Edges)
		if cycle == nil {
			return nil
		}
		custom := customDependencyEdges(cfg, result.Groups)
		for _, edge := range cycle {
			if custom[edge.ID()] {
				return cycleMessage(cycle, custom)
			}
		}
		var candidatesInCycle []model.Edge
		for _, edge := range cycle {
			if acceptedEdges[edge.ID()] {
				candidatesInCycle = append(candidatesInCycle, edge)
			}
		}
		var drop model.Edge
		switch {
		case len(candidatesInCycle) == 1:
			drop = candidatesInCycle[0]
			fmt.Fprintf(writer, "Dropping inferred dependency %s -> %s to resolve a cycle.\n", drop.Consumer.ID(), drop.Provider.ID())
		default:
			options := candidatesInCycle
			if len(options) == 0 {
				options = cycle
			}
			if reader != nil {
				drop, err = promptDropEdge(reader, writer, options)
				if err != nil {
					return err
				}
			} else {
				drop = options[0]
				for _, edge := range options {
					if edge.ID() < drop.ID() {
						drop = edge
					}
				}
				fmt.Fprintf(writer, "Dropping dependency %s -> %s to resolve a cycle.\n", drop.Consumer.ID(), drop.Provider.ID())
			}
		}
		if err := dropDependencyEdge(cfg, result.Groups, drop); err != nil {
			return err
		}
		cfg.Ignore(model.DependencyCandidate{Consumer: drop.Consumer, Provider: drop.Provider})
	}
}

func customDependencyEdges(cfg *config.Config, groups map[string][]model.Ref) map[string]bool {
	ids := make(map[string]bool)
	for _, dependency := range cfg.CustomDependencies {
		for _, consumer := range groups[dependency.Consumer] {
			for _, provider := range groups[dependency.Provider] {
				if consumer.ID() != provider.ID() {
					ids[model.Edge{Consumer: consumer, Provider: provider}.ID()] = true
				}
			}
		}
	}
	return ids
}

func dropDependencyEdge(cfg *config.Config, groups map[string][]model.Ref, edge model.Edge) error {
	for index, dependency := range cfg.Dependencies {
		if refsContain(groups[dependency.Consumer], edge.Consumer) && refsContain(groups[dependency.Provider], edge.Provider) {
			cfg.Dependencies = append(cfg.Dependencies[:index], cfg.Dependencies[index+1:]...)
			return nil
		}
	}
	return fmt.Errorf("could not locate a dependency to drop for %s", edge.ID())
}

func refsContain(refs []model.Ref, target model.Ref) bool {
	for _, ref := range refs {
		if ref.ID() == target.ID() {
			return true
		}
	}
	return false
}

func promptDropEdge(reader *bufio.Reader, writer io.Writer, edges []model.Edge) (model.Edge, error) {
	fmt.Fprintln(writer, "\nThese dependencies form a cycle:")
	for index, edge := range edges {
		fmt.Fprintf(writer, "  %d. %s -> %s [%s]\n", index+1, edge.Consumer.ID(), edge.Provider.ID(), edge.Evidence)
	}
	for {
		fmt.Fprint(writer, "Enter the number of the dependency to drop: ")
		line, err := reader.ReadString('\n')
		number, convErr := strconv.Atoi(strings.TrimSpace(line))
		if convErr == nil && number >= 1 && number <= len(edges) {
			return edges[number-1], nil
		}
		if err != nil && errors.Is(err, io.EOF) {
			return edges[0], nil
		}
		if err != nil {
			return model.Edge{}, err
		}
		fmt.Fprintln(writer, "Invalid selection.")
	}
}

func formatCycle(cycle []model.Edge, custom map[string]bool) string {
	var builder strings.Builder
	for _, edge := range cycle {
		tag := ""
		if custom[edge.ID()] {
			tag = "  (custom dependency)"
		}
		fmt.Fprintf(&builder, "  %s -> %s%s\n", edge.Consumer.ID(), edge.Provider.ID(), tag)
	}
	return builder.String()
}

// cycleMessage renders a dependency cycle, tagging any custom dependencies, with
// a header that reflects whether a custom dependency is the cause.
func cycleMessage(cycle []model.Edge, custom map[string]bool) error {
	header := "configuration produces a dependency cycle"
	for _, edge := range cycle {
		if custom[edge.ID()] {
			header = "custom dependency creates a dependency cycle"
			break
		}
	}
	return fmt.Errorf("%s:\n%s", header, formatCycle(cycle, custom))
}

// buildPlan wraps planner.Build so that a cycle is reported with the shared,
// custom-aware formatting instead of the low-level graph error.
func buildPlan(result *discovery.Result, cfg *config.Config, direction planner.Direction) (*planner.Plan, error) {
	plan, err := planner.Build(result, direction)
	var cycleErr *graph.CycleError
	if errors.As(err, &cycleErr) {
		return nil, cycleMessage(cycleErr.Cycle, customDependencyEdges(cfg, result.Groups))
	}
	return plan, err
}

func validateConfiguration(snapshot *kube.Snapshot, cfg *config.Config) (*planner.Plan, error) {
	result, err := discovery.BuildPlan(snapshot, cfg)
	if err != nil {
		return nil, err
	}
	plan, err := buildPlan(result, cfg, planner.Down)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func printPlanSummary(writer io.Writer, plan *planner.Plan) {
	workloads := 0
	for _, wave := range plan.Waves {
		workloads += len(wave)
	}
	gitOps := 0
	for _, phase := range plan.Before {
		gitOps += len(phase.Actions)
	}
	fmt.Fprintf(writer, "\nProspective down plan: %d workloads, %d waves, %d GitOps resources.\n", workloads, len(plan.Waves), gitOps)
	for _, warning := range plan.Warnings {
		fmt.Fprintf(writer, "  Warning: %s\n", warning)
	}
}

func parseSelection(value string, count int) (map[int]bool, error) {
	selected := make(map[int]bool)
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "none" || value == "" {
		return selected, nil
	}
	if value == "all" {
		for index := 0; index < count; index++ {
			selected[index] = true
		}
		return selected, nil
	}
	for _, raw := range strings.Split(value, ",") {
		number, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil || number < 1 || number > count {
			return nil, fmt.Errorf("invalid suggestion number %q", strings.TrimSpace(raw))
		}
		selected[number-1] = true
	}
	return selected, nil
}

func interactiveTerminal(reader io.Reader) bool {
	file, ok := reader.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

func planExecuteAction(direction planner.Direction, stdin io.Reader, stdout io.Writer) cli.ActionFunc {
	return func(ctx context.Context, command *cli.Command) error {
		opts, err := commandOptions(command)
		if err != nil {
			return err
		}
		result, cfg, client, err := load(ctx, opts)
		if err != nil {
			return err
		}
		plan, err := buildPlan(result, cfg, direction)
		if err != nil {
			return err
		}
		if err := output.RenderPlan(stdout, plan, opts.output); err != nil {
			return err
		}
		if opts.dryRun {
			return nil
		}
		if opts.output != "human" && !opts.yes {
			return fmt.Errorf("--yes is required when applying with non-human output")
		}
		if !opts.yes {
			confirmed, err := confirm(stdin, stdout, direction)
			if err != nil {
				return err
			}
			if !confirmed {
				return fmt.Errorf("operation cancelled")
			}
		}
		operationID, err := newOperationID()
		if err != nil {
			return err
		}
		runner := &executor.Executor{
			Client: client.Interface, Dynamic: client.Dynamic, Parallelism: opts.parallelism,
			Timeout: opts.timeout, Poll: time.Second, Output: stdout, Now: time.Now,
		}
		return runner.Run(ctx, plan, operationID)
	}
}

func commandOptions(command *cli.Command) (options, error) {
	opts, err := connectionOptions(command)
	if err != nil {
		return opts, err
	}
	opts.output = command.String("output")
	opts.parallelism = command.Int("parallelism")
	opts.timeout = command.Duration("timeout")
	opts.yes = command.Bool("yes")
	opts.acceptSuggestions = command.Bool("accept-suggestions")
	opts.ignoreSuggestions = command.Bool("ignore-suggestions")
	opts.dryRun = command.Bool("dry-run")
	if opts.parallelism < 1 {
		return opts, fmt.Errorf("parallelism must be at least 1")
	}
	if opts.timeout <= 0 {
		return opts, fmt.Errorf("timeout must be positive")
	}
	if opts.output != "human" && opts.output != "json" {
		return opts, fmt.Errorf("unsupported output format %q", opts.output)
	}
	if opts.acceptSuggestions && opts.ignoreSuggestions {
		return opts, fmt.Errorf("--accept-suggestions and --ignore-suggestions are mutually exclusive")
	}
	if opts.dryRun && (opts.acceptSuggestions || opts.ignoreSuggestions) {
		return opts, fmt.Errorf("--dry-run cannot be combined with suggestion decisions")
	}
	if opts.output == "json" && (opts.acceptSuggestions || opts.ignoreSuggestions) {
		return opts, fmt.Errorf("suggestion decisions cannot be combined with JSON output")
	}
	return opts, nil
}

func connectionOptions(command *cli.Command) (options, error) {
	opts := options{
		config: command.String("config"), kubeconfig: command.String("kubeconfig"), context: command.String("context"),
	}
	if command.NArg() != 0 {
		return opts, fmt.Errorf("unexpected arguments: %s", strings.Join(command.Args().Slice(), " "))
	}
	return opts, nil
}

func load(ctx context.Context, opts options) (*discovery.Result, *config.Config, *kube.Client, error) {
	cfg, err := config.Load(opts.config)
	if err != nil {
		return nil, nil, nil, err
	}
	snapshot, client, err := loadSnapshot(ctx, opts)
	if err != nil {
		return nil, nil, nil, err
	}
	result, err := discovery.BuildPlan(snapshot, cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	return result, cfg, client, nil
}

func loadDiscovery(ctx context.Context, opts options, allowMissing bool) (*discovery.Result, *kube.Client, *config.Config, *kube.Snapshot, bool, error) {
	cfg, err := config.Load(opts.config)
	inferred := false
	if err != nil && (!allowMissing || !errors.Is(err, os.ErrNotExist)) {
		return nil, nil, nil, nil, false, err
	}
	snapshot, client, err := loadSnapshot(ctx, opts)
	if err != nil {
		return nil, nil, nil, nil, false, err
	}
	if cfg == nil {
		cfg, err = discovery.BootstrapConfig(snapshot)
		if err != nil {
			return nil, nil, nil, nil, false, err
		}
		inferred = true
	}
	cfg.ResetDiscovered()
	result, err := discovery.Build(snapshot, cfg)
	if err != nil {
		return nil, nil, nil, nil, false, err
	}
	return result, client, cfg, snapshot, inferred, nil
}

func loadSnapshot(ctx context.Context, opts options) (*kube.Snapshot, *kube.Client, error) {
	client, err := kube.New(opts.kubeconfig, opts.context)
	if err != nil {
		return nil, nil, err
	}
	snapshot, err := client.Snapshot(ctx)
	if err != nil {
		return nil, nil, err
	}
	return snapshot, client, nil
}

func confirm(reader io.Reader, writer io.Writer, direction planner.Direction) (bool, error) {
	fmt.Fprintf(writer, "\nExecute %s plan? [y/N]: ", direction)
	line, err := bufio.NewReader(reader).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	return answer == "y" || answer == "yes", nil
}

func newOperationID() (string, error) {
	random := make([]byte, 4)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate operation ID: %w", err)
	}
	return time.Now().UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(random), nil
}
