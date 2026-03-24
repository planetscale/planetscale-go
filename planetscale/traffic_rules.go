package planetscale

import (
	"context"
	"net/http"
	"path"
)

var _ TrafficRulesService = &trafficRulesService{}

// TrafficRulesService communicates with the PlanetScale traffic rules API.
type TrafficRulesService interface {
	Create(context.Context, *CreateTrafficRuleRequest) (*TrafficRule, error)
	Delete(context.Context, *DeleteTrafficRuleRequest) error
}

// CreateTrafficRuleRequest is the request for creating a traffic rule on a budget.
type CreateTrafficRuleRequest struct {
	Organization string            `json:"-"`
	Database     string            `json:"-"`
	Branch       string            `json:"-"`
	BudgetID     string            `json:"-"`
	Kind         string            `json:"kind"`
	Fingerprint  *string           `json:"fingerprint,omitempty"`
	Keyspace     *string           `json:"keyspace,omitempty"`
	Tags         *[]TrafficRuleTag `json:"tags,omitempty"`
}

// DeleteTrafficRuleRequest is the request for deleting a traffic rule.
type DeleteTrafficRuleRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	BudgetID     string `json:"-"`
	RuleID       string `json:"-"`
}

type trafficRulesService struct {
	client *Client
}

// NewTrafficRulesService returns a TrafficRulesService backed by client.
func NewTrafficRulesService(client *Client) *trafficRulesService {
	return &trafficRulesService{client: client}
}

func (s *trafficRulesService) Create(ctx context.Context, createReq *CreateTrafficRuleRequest) (*TrafficRule, error) {
	req, err := s.client.newRequest(http.MethodPost, trafficBudgetRulesAPIPath(createReq.Organization, createReq.Database, createReq.Branch, createReq.BudgetID), createReq)
	if err != nil {
		return nil, err
	}

	rule := &TrafficRule{}
	if err := s.client.do(ctx, req, rule); err != nil {
		return nil, err
	}

	return rule, nil
}

func (s *trafficRulesService) Delete(ctx context.Context, deleteReq *DeleteTrafficRuleRequest) error {
	req, err := s.client.newRequest(http.MethodDelete, trafficBudgetRuleAPIPath(deleteReq.Organization, deleteReq.Database, deleteReq.Branch, deleteReq.BudgetID, deleteReq.RuleID), nil)
	if err != nil {
		return err
	}

	return s.client.do(ctx, req, nil)
}

func trafficBudgetRulesAPIPath(org, db, branch, budgetID string) string {
	return path.Join(trafficBudgetAPIPath(org, db, branch, budgetID), "rules")
}

func trafficBudgetRuleAPIPath(org, db, branch, budgetID, ruleID string) string {
	return path.Join(trafficBudgetRulesAPIPath(org, db, branch, budgetID), ruleID)
}
