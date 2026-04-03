## Content for design.md

# AI Control Tower UI Design

## 1. Design Goal
Create a single, easy decision workspace for operations managers who are not technical and need fast, confident action.

## 2. Design Principles
1. Action first, data second
2. One-screen clarity for urgent decisions
3. Explain every recommendation
4. Minimize clicks for high-frequency workflows
5. Show business impact in plain language

## 3. Primary Screens

### 3.1 Control Tower Home
Purpose: immediate awareness of what needs attention now.

Widgets:
1. Critical exceptions count
2. At-risk shipments by region
3. Top 5 recommended actions
4. SLA risk trend
5. Demurrage risk tracker
6. Source-system health indicators

### 3.2 Exception Board
Purpose: ranked list of issues needing intervention.

Columns:
1. Priority score
2. Shipment or container id
3. Issue type
4. Time to SLA breach
5. Recommended action
6. Owner
7. Status

Controls:
1. Filter by region, terminal, customer tier, issue type
2. Sort by risk, cost impact, or due time
3. Bulk assign and bulk acknowledge

### 3.3 Shipment Decision View
Purpose: decide what to do for one shipment.

Sections:
1. Current status timeline
2. Root-cause summary
3. Recommendation options A, B, and C
4. Impact estimate for each option
5. Confidence and assumptions
6. Approve, modify, or reject action
7. Notes and collaboration log

### 3.4 Action Center
Purpose: execute approved decisions.

Capabilities:
1. Assign owner and due time
2. Trigger external workflow calls
3. Track progress and escalation
4. Capture completion outcome

### 3.5 KPI and Insights View
Purpose: leadership and optimization review.

Charts:
1. Exception volume by type
2. Mean resolution time
3. SLA breach trend
4. Cost avoided trend
5. Recommendation acceptance and success rate

## 4. Suggested Information Hierarchy
1. What is wrong
2. What should be done now
3. What happens if no action is taken
4. Who owns the action
5. How this affects cost and customer commitment

## 5. Recommended Visual Language
1. Clean light theme with high contrast
2. Priority colors:
- Critical: red
- High: orange
- Medium: amber
- Low: blue
3. Neutral background with subtle gradient paneling
4. Simple icon set for issue types
5. Clear typography with large numeric indicators

## 6. Interaction Patterns
1. Single-click open of shipment details from exception board
2. Slide-out decision panel for quick actions
3. Compare two recommendations side by side
4. One-click escalation with pre-filled context
5. Inline explanation for each recommendation

## 7. AI Explainability UX
Every recommendation card should show:
1. Why this action is suggested
2. Data sources used
3. Confidence level
4. Estimated gain
5. Risk if ignored

Plain-language example:
1. Problem: likely customs delay in 4 hours
2. Best action: escalate document X to compliance team
3. Impact: reduces SLA breach probability from 62 percent to 18 percent

## 8. Alerts and Notifications
1. In-app critical banner for immediate threats
2. Digest feed every 30 minutes for non-critical updates
3. Role-based notifications for owner assignments
4. Escalation notification when due time is missed

## 9. User Roles and Views
1. Control Tower Manager: full cross-functional board
2. Terminal Planner: terminal and yard focused subset
3. Transport Planner: route and provider focused subset
4. Compliance Officer: hold and documentation focused subset
5. Service Manager: customer-risk and SLA communication subset

## 10. Mobile and Tablet Behavior
1. Mobile: critical alerts, approvals, owner assignment, quick status updates
2. Tablet: exception board and shipment decision card
3. Desktop: full analytics, simulation, and deep investigation

## 11. Accessibility and Usability
1. Keyboard-friendly navigation
2. Color plus icon coding for risk state
3. Clear labels without logistics jargon where possible
4. Progressive disclosure: summary first, detail on demand

## 12. Demo-Ready UI Flow for Hackathon
1. User opens Control Tower Home and sees 3 critical exceptions
2. User opens top exception and reviews AI recommendation
3. User compares Option A vs Option B impact
4. User approves action and assigns owner
5. Dashboard updates projected SLA and cost outcome

## 13. MVP UI Components
1. Exception table
2. Recommendation card
3. Impact estimator
4. Approval modal
5. Task status timeline
6. KPI summary cards
