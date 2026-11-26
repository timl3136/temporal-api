package main

import (
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	nexusannotationsv1 "github.com/nexus-rpc/nexus-proto-annotations/go/nexusannotations/v1"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"gopkg.in/yaml.v3"
)

func TestGetWorkflowExecutionResultContract(t *testing.T) {
	const root = "../.."
	const request = "temporal.api.workflowservice.v1.GetWorkflowExecutionResultRequest"
	const output = "temporal.api.common.v1.Payload"
	path := filepath.Join(t.TempDir(), "workflow-service.pb")
	cmd := exec.Command("protoc", "-I", root, "--include_imports", "--descriptor_set_out="+path,
		"temporal/api/workflowservice/v1/service.proto")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("compile workflow service descriptor: %v\n%s", err, out)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var descriptors descriptorpb.FileDescriptorSet
	if err := proto.Unmarshal(data, &descriptors); err != nil {
		t.Fatal(err)
	}
	files, err := protodesc.NewFiles(&descriptors)
	if err != nil {
		t.Fatal(err)
	}
	desc, err := files.FindDescriptorByName("temporal.api.workflowservice.v1.WorkflowService.GetWorkflowExecutionResult")
	if err != nil {
		t.Fatalf("find result RPC: %v", err)
	}
	rpc := desc.(protoreflect.MethodDescriptor)
	if rpc.Input().FullName() != request || rpc.Output().FullName() != output {
		t.Fatalf("RPC signature = (%s) -> %s, want (%s) -> %s", rpc.Input().FullName(), rpc.Output().FullName(), request, output)
	}
	fields := rpc.Input().Fields()
	if fields.Len() != 1 {
		t.Fatalf("request has %d fields, want 1", fields.Len())
	}
	field := fields.Get(0)
	if field.Name() != "execution" || field.Number() != 1 || field.Cardinality() != protoreflect.Optional ||
		field.Kind() != protoreflect.MessageKind || field.Message().FullName() != "temporal.api.common.v1.WorkflowExecution" {
		t.Fatalf("unexpected execution field: %v", field)
	}
	operation := proto.GetExtension(rpc.Options(), nexusannotationsv1.E_Operation).(*nexusannotationsv1.OperationOptions)
	if !slices.Contains(operation.GetTags(), "exposed") {
		t.Fatal("result RPC is missing the exposed Nexus operation tag")
	}
	types := dynamicpb.NewTypes(files)
	http, err := types.FindExtensionByName("google.api.http")
	if err != nil {
		t.Fatal(err)
	}
	optionsBytes, err := proto.Marshal(rpc.Options())
	if err != nil {
		t.Fatal(err)
	}
	options := dynamicpb.NewMessage(http.TypeDescriptor().ContainingMessage())
	if err := (proto.UnmarshalOptions{Resolver: types}).Unmarshal(optionsBytes, options); err != nil {
		t.Fatal(err)
	}
	if proto.HasExtension(options, http) {
		t.Fatal("result RPC must have no google.api.http binding")
	}

	wit, err := os.ReadFile(filepath.Join(root, "nexus/workflow-service.wit"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(wit), `@nexus.proto "`+request+`"`) {
		t.Fatal("WIT is missing the protobuf request binding")
	}
	signature := regexp.MustCompile(`(?m)^  /// @nexus.operation name="GetWorkflowExecutionResult"
  /// @nexus.experimental
  get-workflow-execution-result: func\(\s*request: get-workflow-execution-result-request,?\s*\) -> payload;`)
	if !signature.Match(wit) {
		t.Fatal("WIT is missing the experimental GetWorkflowExecutionResult operation with its request and payload result")
	}

	t.Run("NexusYAMLOutput", func(t *testing.T) {
		data, err := os.ReadFile(filepath.Join(root, "nexus/temporal-proto-models-nexusrpc.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		var model struct {
			Services map[string]struct {
				Operations map[string]struct {
					Output map[string]string
				}
			}
		}
		if err := yaml.Unmarshal(data, &model); err != nil {
			t.Fatal(err)
		}
		got := model.Services[string(rpc.Parent().FullName())].Operations[string(rpc.Name())].Output
		want := map[string]string{
			"$dotnetRef":     "Temporalio.Api.Common.V1.Payload",
			"$goRef":         "go.temporal.io/api/common/v1.Payload",
			"$javaRef":       "io.temporal.api.common.v1.Payload",
			"$pythonRef":     "temporalio.api.common.v1.Payload",
			"$rubyRef":       "Temporalio::Api::Common::V1::Payload",
			"$typescriptRef": "@temporalio/api/common/v1.Payload",
		}
		if !maps.Equal(got, want) {
			t.Fatalf("Nexus YAML output refs = %v, want %v", got, want)
		}
	})

	testResultSubscriptionRoundTrips(t, files)
}

func testResultSubscriptionRoundTrips(t *testing.T, files *protoregistry.Files) {
	t.Helper()
	const legacy = `nexus { url: "https://example.test/callback" }`
	const subscription = legacy + `
		result_subscription {
			request_id: "original-request"
			result_format_version: 1
			resolved_execution { workflow_id: "target-workflow" run_id: "resolved-run" }
		}`
	for _, tc := range []struct {
		name string
		desc protoreflect.FullName
		text string
	}{
		{"LegacyCallback", "temporal.api.common.v1.Callback", legacy},
		{"SubscriptionCallback", "temporal.api.common.v1.Callback", subscription},
		{"InheritedCallback", "temporal.api.history.v1.WorkflowExecutionStartedEventAttributes",
			`completion_callbacks { ` + subscription + ` }`},
		{"CanceledSubscriptions", "temporal.api.history.v1.WorkflowExecutionOptionsUpdatedEventAttributes",
			`canceled_result_subscription_request_ids: "original-request"
			 canceled_result_subscription_request_ids: "other-request"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			desc, err := files.FindDescriptorByName(tc.desc)
			if err != nil {
				t.Fatal(err)
			}
			msgType := desc.(protoreflect.MessageDescriptor)
			want := dynamicpb.NewMessage(msgType)
			if err := prototext.Unmarshal([]byte(tc.text), want); err != nil {
				t.Fatal(err)
			}
			data, err := proto.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			got := dynamicpb.NewMessage(msgType)
			if err := proto.Unmarshal(data, got); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(got, want) {
				t.Fatalf("binary round trip = %v, want %v", got, want)
			}
		})
	}
}
