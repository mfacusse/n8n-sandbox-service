// Package azurescale wraps the Azure VM Scale Set control-plane API behind a
// small interface, so the capacity scaler's decision logic can be tested
// without live Azure credentials or network access.
package azurescale

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute"
)

// VMSSScaler reads and sets the instance count of one Azure Virtual Machine
// Scale Set.
type VMSSScaler interface {
	// CurrentCapacity returns the scale set's current instance count.
	CurrentCapacity(ctx context.Context) (int, error)
	// SetCapacity requests the scale set be resized to target instances. It
	// blocks until Azure reports the resize complete or ctx is done.
	SetCapacity(ctx context.Context, target int) error
}

// Target identifies the single VMSS a VMSSScaler operates on.
type Target struct {
	SubscriptionID string
	ResourceGroup  string
	VMSSName       string

	// WorkloadIdentityClientID and TenantID select the federated identity used
	// to authenticate. Empty values fall back to the ambient AZURE_CLIENT_ID /
	// AZURE_TENANT_ID environment variables set by the Azure workload identity
	// webhook.
	WorkloadIdentityClientID string
	TenantID                 string
}

// azureVMSSScaler is the armcompute/azidentity-backed VMSSScaler.
type azureVMSSScaler struct {
	client *armcompute.VirtualMachineScaleSetsClient
	target Target
}

// New builds a VMSSScaler authenticated via Azure workload identity, scoped
// to the single VMSS named in target.
func New(target Target) (VMSSScaler, error) {
	cred, err := azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
		ClientID: target.WorkloadIdentityClientID,
		TenantID: target.TenantID,
	})
	if err != nil {
		return nil, fmt.Errorf("azurescale: workload identity credential: %w", err)
	}
	client, err := armcompute.NewVirtualMachineScaleSetsClient(target.SubscriptionID, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("azurescale: vmss client: %w", err)
	}
	return &azureVMSSScaler{client: client, target: target}, nil
}

func (a *azureVMSSScaler) CurrentCapacity(ctx context.Context) (int, error) {
	resp, err := a.client.Get(ctx, a.target.ResourceGroup, a.target.VMSSName, nil)
	if err != nil {
		return 0, fmt.Errorf("azurescale: get vmss: %w", err)
	}
	if resp.SKU == nil || resp.SKU.Capacity == nil {
		return 0, fmt.Errorf("azurescale: vmss %s/%s has no sku capacity", a.target.ResourceGroup, a.target.VMSSName)
	}
	return int(*resp.SKU.Capacity), nil
}

func (a *azureVMSSScaler) SetCapacity(ctx context.Context, target int) error {
	capacity := int64(target)
	poller, err := a.client.BeginUpdate(ctx, a.target.ResourceGroup, a.target.VMSSName, armcompute.VirtualMachineScaleSetUpdate{
		SKU: &armcompute.SKU{Capacity: &capacity},
	}, nil)
	if err != nil {
		return fmt.Errorf("azurescale: begin update vmss: %w", err)
	}
	if _, err := poller.PollUntilDone(ctx, nil); err != nil {
		return fmt.Errorf("azurescale: update vmss: %w", err)
	}
	return nil
}
