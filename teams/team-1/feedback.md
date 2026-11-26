# Professor Feedback — Citi Bike MVP

**Project:** DATA 622 — Team 1  
**Presentation:** Will There Be a Bike When I Get There?

## Objective

Move the existing Citi Bike MVP toward a scalable, automated, and economically sustainable production workflow in Databricks.

## 1. GitHub and Production Workflow

- [ ] Merge `main` into the Team One branch.
- [ ] Conduct development and experimentation in the team branch.
- [ ] Identify the official production notebooks.
- [ ] Submit changes to `main` through pull requests for professor approval.

## 2. Full-Network Coverage

- [ ] Expand the workflow from 100 stations to the full network (approximately 2,500 stations).
- [ ] Train and evaluate models using the expanded dataset.
- [ ] Measure runtime and computational cost at full-network scale.

## 3. Weather Integration

- [ ] Assign ownership of weather data integration.
- [ ] Expand the available weather features.
- [ ] Evaluate whether weather features improve model performance.
- [ ] Ensure weather inputs are available at prediction time.

## 4. Exploratory Data Analysis (EDA)

- [ ] Inspect data quality, missing values, duplicates, and feature distributions.
- [ ] Analyze empty and full station events and state transitions.
- [ ] Investigate hourly, daily, and station-level patterns.
- [ ] Examine predictor relationships, correlations, and weather effects.
- [ ] Produce visualizations and document findings that inform model development.

## 5. Model Training and Evaluation

- [ ] Reproduce the existing logistic regression and histogram gradient boosting results.
- [ ] Investigate additional features and candidate models.
- [ ] Improve detection of new empty and full events.
- [ ] Evaluate PR-AUC, Brier score, precision, recall, and calibration.
- [ ] Verify temporal data leakage prevention.
- [ ] Schedule a weekly model training job.
- [ ] Compare newly trained models against the current production model before replacement.

## 6. Computational Costs and Economics

- [ ] Measure the computational cost of data collection, feature engineering, training, and inference.
- [ ] Compare costs for 100 stations and the full network.
- [ ] Estimate daily, weekly, and monthly operating costs.
- [ ] Evaluate the economics of weekly training and predictions every 15 minutes.
- [ ] Recommend a sustainable execution schedule based on cost, performance, and data freshness.

## Individual Responsibilities

| Owner | Workstream |
| --- | --- |
| Candace | Exploratory data analysis, feature investigation, model training and evaluation |
| TBD | Full-network data integration |
| TBD | Weather integration |
| TBD | Scheduled training and production workflows |
| TBD | Computational cost analysis |

## Existing Performance Baseline

| Metric | Current result |
| --- | ---: |
| Stations | 100 |
| State unchanged after 15 minutes | 94.9% |
| Gradient boosting — empty PR-AUC | 0.881 |
| Gradient boosting — full PR-AUC | 0.974 |
| Gradient boosting — empty Brier score | 0.0114 |
| Gradient boosting — full Brier score | 0.0312 |
| New empty event recall | 17% |
| New empty event precision | 44% |

Source: Team 1 MVP presentation, November 20, 2026; historical test week October 19–26.

## Completion Criteria

The team can demonstrate full-network coverage, documented EDA, evaluated models, automated weekly training, and measured operational costs before submitting the production workflow for professor approval.

## Scope Notes

Branch management, network expansion, EDA, and computational cost measurement were explicitly discussed in the supplied meeting transcript. Weekly training, expanded weather features, and training/inference economics also reflect the team's recollection of the meeting. Detailed checklist items are proposed implementation steps, not verbatim professor instructions. Remaining ownership assignments are to be confirmed by the team.
