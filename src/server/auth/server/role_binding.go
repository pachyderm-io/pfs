package server

import (
	"context"
	"fmt"

	"github.com/pachyderm/pachyderm/v2/src/auth"
)

type groupLookupFn func(ctx context.Context, subject string) ([]string, error)

type authorizeRequest struct {
	subject              string
	permissions          map[auth.Permission]bool
	satisfiedPermissions []auth.Permission
	groupsForSubject     groupLookupFn
	groups               []string
}

func newAuthorizeRequest(subject string, permissions []auth.Permission, groupsForSubject groupLookupFn) *authorizeRequest {
	permissionMap := make(map[auth.Permission]bool)
	for _, p := range permissions {
		permissionMap[p] = true
	}

	return &authorizeRequest{
		subject:          subject,
		permissions:      permissionMap,
		groupsForSubject: groupsForSubject,
	}
}

// satisfied returns true if no permissions remain
func (r *authorizeRequest) satisfied() bool {
	return len(r.permissions) == 0
}

func (r *authorizeRequest) missing() []auth.Permission {
	missing := make([]auth.Permission, 0, len(r.permissions))
	for p := range r.permissions {
		missing = append(missing, p)
	}
	return missing
}

// evaluateRoleBinding removes permissions that are satisfied by the role binding from the
// set of desired permissions.
func (r *authorizeRequest) evaluateRoleBinding(ctx context.Context, binding *auth.RoleBinding) error {
	if err := r.evaluateRoleBindingForSubject(r.subject, binding); err != nil {
		return err
	}

	if len(r.permissions) == 0 {
		return nil
	}

	if r.groups == nil {
		var err error
		r.groups, err = r.groupsForSubject(ctx, r.subject)
		if err != nil {
			return err
		}
	}

	for _, g := range r.groups {
		if err := r.evaluateRoleBindingForSubject(g, binding); err != nil {
			return err
		}
	}

	return nil
}

func (r *authorizeRequest) evaluateRoleBindingForSubject(subject string, binding *auth.RoleBinding) error {
	if binding.Entries == nil {
		return nil
	}

	if entry, ok := binding.Entries[subject]; ok {
		for role := range entry.Roles {
			permissions, err := permissionsForRole(role)
			if err != nil {
				return err
			}

			for _, permission := range permissions {
				if _, ok := r.permissions[permission]; ok {
					r.satisfiedPermissions = append(r.satisfiedPermissions, permission)
					delete(r.permissions, permission)
				}
			}
		}
	}
	return nil
}

// permissionsForRole returns the set of permissions associated with a role.
// For now this is a hard-coded list but it may be extended to support user-defined roles.
func permissionsForRole(role string) ([]auth.Permission, error) {
	switch role {
	case auth.ClusterAdminRole:
		return []auth.Permission{
			auth.Permission_CLUSTER_ADMIN,
		}, nil
	case auth.RepoOwnerRole:
		return []auth.Permission{
			auth.Permission_REPO_READ,
			auth.Permission_REPO_WRITE,
			auth.Permission_REPO_MODIFY_BINDINGS,
		}, nil
	case auth.RepoWriterRole:
		return []auth.Permission{
			auth.Permission_REPO_READ,
			auth.Permission_REPO_WRITE,
		}, nil
	case auth.RepoReaderRole:
		return []auth.Permission{
			auth.Permission_REPO_READ,
			auth.Permission_REPO_WRITE,
		}, nil
	}
	return nil, fmt.Errorf("unknown role %q", role)
}
