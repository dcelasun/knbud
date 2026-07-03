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
	"strings"
	"time"

	"github.com/dcelasun/knbud/internal/config"
	"github.com/dcelasun/knbud/internal/discovery"
	"github.com/dcelasun/knbud/internal/executor"
	"github.com/dcelasun/knbud/internal/kube"
	"github.com/dcelasun/knbud/internal/output"
	"github.com/dcelasun/knbud/internal/planner"
	"github.com/urfave/cli/v3"
)

type options struct {
	config      string
	kubeconfig  string
	context     string
	output      string
	parallelism int
	timeout     time.Duration
	yes         bool
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
				Flags:  commonFlags(false),
				Action: discoverAction(stdout),
			},
			{
				Name:  "plan",
				Usage: "Build a read-only maintenance plan",
				Commands: []*cli.Command{
					{Name: "down", Usage: "Plan shutdown ordering", Flags: commonFlags(false), Action: planAction(planner.Down, stdout)},
					{Name: "up", Usage: "Plan restoration ordering", Flags: commonFlags(false), Action: planAction(planner.Up, stdout)},
				},
			},
			{Name: "down", Usage: "Stop NFS-dependent workloads", Flags: commonFlags(true), Action: executeAction(planner.Down, stdin, stdout)},
			{Name: "up", Usage: "Restore NFS-dependent workloads", Flags: commonFlags(true), Action: executeAction(planner.Up, stdin, stdout)},
		},
	}
	return command
}

func commonFlags(mutable bool) []cli.Flag {
	flags := []cli.Flag{
		&cli.StringFlag{Name: "config", Value: "knbud.yaml", Usage: "Configuration file"},
		&cli.StringFlag{Name: "kubeconfig", Usage: "Kubeconfig file"},
		&cli.StringFlag{Name: "context", Usage: "Kubeconfig context"},
		&cli.StringFlag{Name: "output", Value: "human", Usage: "Output format: human or json"},
		&cli.IntFlag{Name: "parallelism", Value: 8, Usage: "Maximum concurrent operations"},
		&cli.DurationFlag{Name: "timeout", Value: 5 * time.Minute, Usage: "Timeout per workload"},
	}
	if mutable {
		flags = append(flags, &cli.BoolFlag{Name: "yes", Usage: "Skip confirmation"})
	}
	return flags
}

func discoverAction(stdout io.Writer) cli.ActionFunc {
	return func(ctx context.Context, command *cli.Command) error {
		opts, err := commandOptions(command)
		if err != nil {
			return err
		}
		result, _, err := load(ctx, opts)
		if err != nil {
			return err
		}
		return output.Discovery(stdout, result, opts.output)
	}
}

func planAction(direction planner.Direction, stdout io.Writer) cli.ActionFunc {
	return func(ctx context.Context, command *cli.Command) error {
		opts, err := commandOptions(command)
		if err != nil {
			return err
		}
		result, _, err := load(ctx, opts)
		if err != nil {
			return err
		}
		plan, err := planner.Build(result, direction)
		if err != nil {
			return err
		}
		return output.RenderPlan(stdout, plan, opts.output)
	}
}

func executeAction(direction planner.Direction, stdin io.Reader, stdout io.Writer) cli.ActionFunc {
	return func(ctx context.Context, command *cli.Command) error {
		opts, err := commandOptions(command)
		if err != nil {
			return err
		}
		result, client, err := load(ctx, opts)
		if err != nil {
			return err
		}
		plan, err := planner.Build(result, direction)
		if err != nil {
			return err
		}
		if err := output.RenderPlan(stdout, plan, opts.output); err != nil {
			return err
		}
		if opts.output != "human" && !opts.yes {
			return fmt.Errorf("--yes is required when executing with non-human output")
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
	opts := options{
		config: command.String("config"), kubeconfig: command.String("kubeconfig"),
		context: command.String("context"), output: command.String("output"),
		parallelism: command.Int("parallelism"), timeout: command.Duration("timeout"),
		yes: command.Bool("yes"),
	}
	if command.NArg() != 0 {
		return opts, fmt.Errorf("unexpected arguments: %s", strings.Join(command.Args().Slice(), " "))
	}
	if opts.parallelism < 1 {
		return opts, fmt.Errorf("parallelism must be at least 1")
	}
	if opts.timeout <= 0 {
		return opts, fmt.Errorf("timeout must be positive")
	}
	if opts.output != "human" && opts.output != "json" {
		return opts, fmt.Errorf("unsupported output format %q", opts.output)
	}
	return opts, nil
}

func load(ctx context.Context, opts options) (*discovery.Result, *kube.Client, error) {
	cfg, err := config.Load(opts.config)
	if err != nil {
		return nil, nil, err
	}
	client, err := kube.New(opts.kubeconfig, opts.context)
	if err != nil {
		return nil, nil, err
	}
	snapshot, err := client.Snapshot(ctx)
	if err != nil {
		return nil, nil, err
	}
	result, err := discovery.Build(snapshot, cfg)
	if err != nil {
		return nil, nil, err
	}
	return result, client, nil
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
