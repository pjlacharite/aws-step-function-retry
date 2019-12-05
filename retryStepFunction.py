import argparse
import json
import datetime
import pytz
import boto3

client = boto3.client('stepfunctions')

def findFailedExecutionAtPage(arn, page, statu):
    '''
    Returns 100 StepFunction execution with given status starting at the page token provided.
    If no page token is provided, returns the first 100 results and the nextPage token.
    '''
    if page is None:
        return client.list_executions(
            stateMachineArn=arn,
            statusFilter=status,
            maxResults=100
        )
    else:
        return client.list_executions(
            stateMachineArn=arn,
            statusFilter=status,
            maxResults=100,
            nextToken=page
        )

def filterByDate(executions, date):
    return list(filter(lambda execution: (execution['startDate'] > date), executions))

def extractArn(executions):
    return list(map(lambda execution: (execution['executionArn']), executions))

def startExecutionAtFailedState(executionArn, newMachine):
    '''
    1- Gets the failed input and state from the executionArn
    '''
    failedSMInfo = parseFailureHistory(executionArn)
    '''
    2- Trigger a new execution with a GoTo statement to the failed state with the failed input.
    '''
    name = json.loads(failedSMInfo[1])["_id"]
    failedInput = json.loads(failedSMInfo[1])
    failedInput["goto"] = failedSMInfo[0]

    try:
        response = client.start_execution(
            stateMachineArn= args.stepFunctionArn + '-with-GoToState',
            name=name,
            input=json.dumps(failedInput)
        )
        print("Execution response: {}".format(response))
        return response
    except client.exceptions.ExecutionAlreadyExists:
        print("The execution with name {} has already been retried.".format(name))

def parseFailureHistory(failedExecutionArn):
    '''
    Parses the execution history of a failed state machine to get the name of failed state and
    the input to the failed state
    Input failedExecutionArn - a string containing the execution Arn of a failed state machine
    Output - a list with two elements: [name of failed state, input to failed state]
    '''

    failedEvents = list()
    failedAtParallelState = False

    try:
        # Get the execution history
        response = client.get_execution_history(
            executionArn=failedExecutionArn,
            reverseOrder=True
        )
        next_token = response.get('nextToken')
        failedEvents.extend(response['events'])
    except Exception as ex:
        raise ex

    while next_token is not None:
        try:
            # Get the execution history
            response = client.get_execution_history(
                executionArn=failedExecutionArn,
                reverseOrder=True,
                nextToken=next_token
            )
            next_token = response.get('nextToken')
            failedEvents.extend(response['events'])
        except Exception as ex:
            raise ex

    # Confrim that the execution actually failed, raise exception if it didn't fail
    try:
        failedEvents[0]['executionFailedEventDetails']
    except:
        raise ('Execution did not fail')
    '''
    If we have a 'States.Runtime' error (for example if a task state in our state 
    machine attempts to execute a lambda function in a different region than the 
    state machine, get the id of the failed state, use id of the failed state to
    determine failed state name and input
    '''
    if failedEvents[0]['executionFailedEventDetails']['error'] == 'States.Runtime':
        failedId = int(filter(str.isdigit, str(failedEvents[0]['executionFailedEventDetails']['cause'].split()[13])))
        failedState = failedEvents[-1 * failedId]['stateEnteredEventDetails']['name']
        failedInput = failedEvents[-1 * failedId]['stateEnteredEventDetails']['input']
        return (failedState, failedInput)
    '''
    We need to loop through the execution history, tracing back the executed steps
    The first state we encounter will be the failed state
    If we failed on a parallel state, we need the name of the parallel state rather than the 
    name of a state within a parallel state it failed on. This is because we can only attach
    the goToState to the parallel state, but not a sub-state within the parallel state.
    This loop starts with the id of the latest event and uses the previous event id's to trace
    back the execution to the beginning (id 0). However, it will return as soon it finds the name
    of the failed state 
    '''
    currentEventId = failedEvents[0]['id']
    while currentEventId != 0:
        # multiply event id by -1 for indexing because we're looking at the reversed history
        currentEvent = failedEvents[-1 * currentEventId]
        '''
        We can determine if the failed state was a parallel state because it an event
        with 'type'='ParallelStateFailed' will appear in the execution history before
        the name of the failed state
        '''
        if currentEvent['type'] == 'ParallelStateFailed':
            failedAtParallelState = True
        '''
        If the failed state is not a parallel state, then the name of failed state to return
        will be the name of the state in the first 'TaskStateEntered' event type we run into 
        when tracing back the execution history
        '''
        if currentEvent['type'] == 'TaskStateEntered' and failedAtParallelState == False:
            failedState = currentEvent['stateEnteredEventDetails']['name']
            failedInput = currentEvent['stateEnteredEventDetails']['input']
            return (failedState, failedInput)
        '''
        If the failed state was a paralell state, then we need to trace execution back to 
        the first event with 'type'='ParallelStateEntered', and return the name of the state
        '''
        if currentEvent['type'] == 'ParallelStateEntered' and failedAtParallelState:
            failedState = failedState = currentEvent['stateEnteredEventDetails']['name']
            failedInput = currentEvent['stateEnteredEventDetails']['input']
            return (failedState, failedInput)
        # Update the id for the next execution of the loop
        currentEventId = currentEvent['previousEventId']

def createGoToStateMachine(stateMachineArn):
    '''
    Note: Per Boto3 documentation, CreateStateMachine is an idempotent API. 
    Calling it with the same name and definition won't duplicate the StepFunction

    Given a state machine arn and the name of a state in that state machine, create a new state machine 
    that starts at a new choice state called the 'GoToState'. The "GoToState" will branch to the named
    state, and send the input of the state machine to that state, when a variable called "resuming" is 
    set to True
    Input   stateMachineArn - string with the Arn of the state machine
    Output  response from the create_state_machine call, which is the API call that creates a new state machine
    '''
    try:
        response = client.describe_state_machine(
            stateMachineArn=stateMachineArn
        )
    except:
        raise ('Could not get ASL definition of state machine')
    roleArn = response['roleArn']
    stateMachine = json.loads(response['definition'])
    # Create a name for the new state machine
    newName = response['name'] + '-with-GoToState'
    # Get the StartAt state for the original state machine, because we will point the 'GoToState' to this state
    originalStartAt = stateMachine['StartAt']

    '''
    Create the GoToState with the variable $.goto
    If new state machine is executed with $.goto = State, then the state machine will skip to the provided goto state
    Otherwise, it will execute the state machine from the original start state
    '''

    choices = []
    for state in stateMachine['States'].keys():
        choices.append({'Variable': '$.goto', 'StringEquals': state, 'Next': state})

    goToState = {'Type': 'Choice',
                 'Choices': choices,
                 'Default': originalStartAt}
    # Add GoToState to the set of states in the new state machine
    stateMachine['States']['GoToState'] = goToState
    # Add StartAt
    stateMachine['StartAt'] = 'GoToState'
    # Create new state machine
    try:
        response = client.create_state_machine(
            name=newName,
            definition=json.dumps(stateMachine),
            roleArn=roleArn
        )
        return response
    except client.exceptions.StateMachineAlreadyExists:
        print('Machine already exists, nothing to do.')
    except:
        raise BaseException('Failed to create new state machine with GoToState')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Execution Arn of the failed state machine.')
    parser.add_argument('--stepFunctionArn', dest='stepFunctionArn', type=str)
    parser.add_argument('--date', dest='date', type=lambda d: datetime.datetime.strptime(d,"%Y-%m-%d"))
    parser.add_argument('--status', dest='status', type=str, nargs='?', const="FAILED")
    
    EST = pytz.timezone('America/Montreal')

    args = parser.parse_args()

    result = {'nextToken': None}
    status = args.status or "FAILED"
    newMachine = createGoToStateMachine(args.stepFunctionArn)
    while True:
        result = findFailedExecutionAtPage(args.stepFunctionArn, result['nextToken'] or None, status)
        filteredExecutions = filterByDate(result['executions'], EST.localize(args.date))
        extractedArns = extractArn(filteredExecutions)
        
        for extractedArn in extractedArns:
            print("Retrying execution ARN: {}".format(extractArn))
            startExecutionAtFailedState(extractedArn, newMachine)

        if "nextToken" in result and extractedArns:
            break