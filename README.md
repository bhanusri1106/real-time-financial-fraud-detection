### Sample Test Results

**Test 1**

| Field             | Result                    |
| ----------------- | ------------------------- |
| Transaction Type  | TRANSFER                  |
| Amount            | 1,000,000                 |
| Fraud Probability | 0.000024                  |
| Prediction        | 0 — Not detected as fraud |

**Test 2**

| Field             | Result                    |
| ----------------- | ------------------------- |
| Transaction Type  | TRANSFER                  |
| Amount            | 2,000,000                 |
| Fraud Probability | 0.823884                  |
| Prediction        | 0 — Not detected as fraud |

**Note:** These are two test transactions, not a measurement of overall model accuracy. The configured fraud threshold is 0.99, so neither prediction crossed the threshold.

