package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	nexusannotationsv1 "github.com/nexus-rpc/nexus-proto-annotations/go/nexusannotations/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
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
}
