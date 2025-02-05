package scale

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/completion"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/templates"
)

type ScaleClusterOptions struct {
	configFlags *genericclioptions.ConfigFlags
	ioStreams   *genericclioptions.IOStreams
	replicas    *int32
	workerGroup string
	cluster     string
}

var (
	scaleLong = templates.LongDesc(`
		Scale a Ray cluster's worker group.
	`)

	scaleExample = templates.Examples(`
		# Scale a Ray cluster's worker group to 3 replicas
		kubectl ray scale my-workergroup --raycluster my-raycluster --replicas 3
	`)
)

func NewScaleClusterOptions(streams genericclioptions.IOStreams) *ScaleClusterOptions {
	return &ScaleClusterOptions{
		configFlags: genericclioptions.NewConfigFlags(true),
		ioStreams:   &streams,
		replicas:    new(int32),
	}
}

func NewScaleClusterCommand(streams genericclioptions.IOStreams) *cobra.Command {
	options := NewScaleClusterOptions(streams)
	// Initialize the factory for later use with the current config flag
	cmdFactory := cmdutil.NewFactory(options.configFlags)

	cmd := &cobra.Command{
		Use:               "scale [WORKERGROUP] [-c/--raycluster CLUSTERNAME] [-r/--replicas N]",
		Short:             "Scale Ray cluster",
		Long:              scaleLong,
		Example:           scaleExample,
		SilenceUsage:      true,
		ValidArgsFunction: completion.RayClusterResourceNameCompletionFunc(cmdFactory),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Complete(cmd, args); err != nil {
				return err
			}
			if err := options.Validate(); err != nil {
				return err
			}
			k8sClient, err := client.NewClient(cmdFactory)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}
			return options.Run(cmd.Context(), k8sClient, os.Stdout)
		},
	}

	cmd.Flags().StringVarP(&options.cluster, "raycluster", "c", "", "Ray cluster name")
	cmd.Flags().Int32VarP(options.replicas, "replicas", "r", -1, "Desired number of replicas in worker group")
	options.configFlags.AddFlags(cmd.Flags())
	return cmd
}

func (options *ScaleClusterOptions) Complete(cmd *cobra.Command, args []string) error {
	if *options.configFlags.Namespace == "" {
		*options.configFlags.Namespace = "default"
	}

	if len(args) != 1 {
		return cmdutil.UsageErrorf(cmd, "%s", cmd.Use)
	}
	options.workerGroup = args[0]

	return nil
}

func (options *ScaleClusterOptions) Validate() error {
	config, err := options.configFlags.ToRawKubeConfigLoader().RawConfig()
	if err != nil {
		return fmt.Errorf("error retrieving raw config: %w", err)
	}

	if !util.HasKubectlContext(config, options.configFlags) {
		return fmt.Errorf("no context is currently set, use %q or %q to select a new one", "--context", "kubectl config use-context <context>")
	}

	if options.cluster == "" {
		return fmt.Errorf("must specify -c/--raycluster")
	}

	if options.replicas == nil || *options.replicas < 0 {
		return fmt.Errorf("must specify -r/--replicas with a non-negative integer")
	}

	return nil
}

func (options *ScaleClusterOptions) Run(ctx context.Context, k8sClient client.Client, writer io.Writer) error {
	cluster, err := k8sClient.RayClient().RayV1().RayClusters(*options.configFlags.Namespace).Get(ctx, options.cluster, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to scale worker group %s in Ray cluster %s in namespace %s: %w", options.workerGroup, options.cluster, *options.configFlags.Namespace, err)
	}

	// find the index of the worker group
	var workerGroups []string
	workerGroupIndex := -1
	for i, workerGroup := range cluster.Spec.WorkerGroupSpecs {
		workerGroups = append(workerGroups, workerGroup.GroupName)
		if workerGroup.GroupName == options.workerGroup {
			workerGroupIndex = i
		}
	}
	if workerGroupIndex == -1 {
		return fmt.Errorf("worker group %s not found in Ray cluster %s in namespace %s. Available worker groups: %s", options.workerGroup, options.cluster, *options.configFlags.Namespace, strings.Join(workerGroups, ", "))
	}

	previousReplicas := *cluster.Spec.WorkerGroupSpecs[workerGroupIndex].Replicas
	if previousReplicas == *options.replicas {
		fmt.Fprintf(writer, "worker group %s in Ray cluster %s in namespace %s already has %d replicas. Skipping\n", options.workerGroup, options.cluster, *options.configFlags.Namespace, previousReplicas)
		return nil
	}

	patch := []byte(fmt.Sprintf(`[{"op": "replace", "path": "/spec/workerGroupSpecs/%d/replicas", "value": %d}]`, workerGroupIndex, *options.replicas))

	// find the worker group and scale it by patching the RayCluster K8s resource
	_, err = k8sClient.RayClient().RayV1().RayClusters(*options.configFlags.Namespace).Patch(ctx, options.cluster, types.JSONPatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to scale worker group %s in Ray cluster %s in namespace %s: %w", options.workerGroup, options.cluster, *options.configFlags.Namespace, err)
	}

	fmt.Fprintf(writer, "Scaled worker group %s in Ray cluster %s in namespace %s from %d to %d replicas\n", options.workerGroup, options.cluster, *options.configFlags.Namespace, previousReplicas, *options.replicas)
	return nil
}
