# aws-step-function-retry
Generates a new StepFunction with an initial Goto state and can automatically retry failed executions at their failed state with the original payload.

# Installation

Make sure your AWS config is valid, otherwise configure it.

```bash
aws configure
```

Install the required python dependencies
```python
pip install pytz
pip install boto3
```

# Usage
## Mandatory Arguments
The stepFunctionArn we wish to retry
```bash
--stepFunctionArn  'arn:aws:states:us-east-1:[AWS Account]]:stateMachine:[StepFunctionName]]'
```
The function will process all failed execution from date to now.
```bash
--date '2019-11-11'
```
## Optional Argument
The Status of the executions we wish to retrieve. Must be 'RUNNING'|'SUCCEEDED'|'FAILED'|'TIMED_OUT'|'ABORTED'. Defaults to 'FAILED'
```bash
--status 'TIMED_OUT'
```

## Example: 
```bash
python retryStepFunction.py --date '2019-11-11' --stepFunctionArn 'arn:aws:states:us-east-1:[AWS Account]]:stateMachine:[StepFunctionName]]'
```

# License and Copyright

Based on the excellent script by AWS Labs [https://github.com/awslabs/aws-sfn-resume-from-any-state](https://github.com/awslabs/aws-sfn-resume-from-any-state). 

This new version creates a single StepFunction that can retry any step of the step function.
It then iterates over all execution with a specific state (default: FAILED) that happened since a given date.
For each failed execution, it finds the failing state and input and retries it on the new machine at the failed state.

Because this script reuses the same execution name as the initial execution, each execution can only be retried once. 
If multiple retries are needed, just change the naming convention on the script.