from requests.auth import HTTPBasicAuth
import requests
import subprocess
import json
import sys
import os
import pathlib

# Fetch API credentials from environment variables
api_token = os.getenv('UPDATE_JIRA_PIRG_API_TOKEN')
email = os.getenv('UPDATE_JIRA_PIRG_API_EMAIL')

# Exit if environment variables are not set
if not api_token or not email:
    print("Error: Missing required environment variables.", file=sys.stderr)
    sys.exit(1)

def test_auth():
    """
    Test the authentication credentials by calling the JIRA API's /myself endpoint.
    Prints the response and status to help debug API access issues.
    """
    url = "https://hpcrcf.atlassian.net/rest/api/3/myself"
    response = requests.get(url, auth=HTTPBasicAuth(email, api_token))
    
    print(f"[AUTH DEBUG] Status Code: {response.status_code}")
    if response.status_code == 200:
        print("[AUTH SUCCESS] Auth is valid.")
        print(json.dumps(response.json(), indent=2))
    else:
        print(f"[AUTH FAIL] Response: {response.text}")

def list_jira_fields():
    """
    Lists all custom and standard fields available in the JIRA instance.
    Useful for debugging field IDs when constructing queries.
    """
    url = "https://hpcrcf.atlassian.net/rest/api/3/field"
    response = requests.get(url, auth=HTTPBasicAuth(email, api_token))
    fields = response.json()
    for field in fields:
        print(f"{field['id']} - {field['name']}")

def pull_filtered_tickets():
    """
    Queries JIRA for all open, unassigned 'Account Request' issues in the TCP project.
    Returns:
        A list of extracted ticket details in the form:
        [[first_name, last_name, pirg, jira_key], ...]
    """
    url = "https://hpcrcf.atlassian.net/rest/api/3/search/jql"
    auth = HTTPBasicAuth(email, api_token)
    headers = {
        "Accept": "application/json"
    }

    jql_string = 'project = TCP AND issuetype = "Account Request" AND status = "Open" AND assignee = EMPTY'

    params = {
        "jql": jql_string,
        'maxResults': 50,
        'fields': 'summary,status,reporter,created, customfield_10401, customfield_10403, customfield_10400',
    }

    response = requests.get(url, headers=headers, params=params, auth=auth)

    if response.status_code != 200:
        print(f"[ERROR] {response.status_code}: {response.text}", file=sys.stderr)
        sys.exit(1)
    
    issues = response.json().get("issues", [])
    
    if not issues:
        print("[DEBUG] No issues returned.")
    else:
        print(f"[DEBUG] Found {len(issues)} issues.")

    input_arr = []

    for issue in issues:
        key = issue["key"]
        fields = issue["fields"]
        summary = fields["summary"]
        status = fields["status"]["name"]
        pirg = fields["customfield_10401"]
        duckID = fields["customfield_10403"]
        created = fields["created"]

        reporter = fields.get("reporter", None)
        reporter_name = (reporter["displayName"] if reporter else "Empty").split()
        reporter_first = reporter_name[0]
        reporter_last = reporter_name[-1]

        pirg_clean = pirg.get("value") if pirg else "None"
    
        print(f"{key:10} | {status:10} | {reporter_first} | {reporter_last} | {duckID} | {pirg_clean} | {summary} | {created}")

        input_arr.append([reporter_first, reporter_last, pirg_clean, key])
    return input_arr

def change_ticket_status(key):
    """
    Transitions a JIRA ticket to the 'Waiting for customer' status
    and assigns it to the authenticated user.

    Args:
        key (str): JIRA ticket key (e.g., TCP-123)
    """
    auth = HTTPBasicAuth(email, api_token)
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    # Step 1: Get all possible transitions for the issue
    transitions_url = f"https://hpcrcf.atlassian.net/rest/api/3/issue/{key}/transitions"
    transitions_response = requests.get(transitions_url, headers=headers, auth=auth)

    if transitions_response.status_code != 200:
        print(f"[ERROR] Could not fetch transitions for {key}: {transitions_response.text}", file=sys.stderr)
        return

    transitions = transitions_response.json().get("transitions", [])
    work_progress_transition_id = None

    # Step 2: Find 'Waiting for customer' transition ID
    for t in transitions:
        if t["name"].lower() == "waiting for customer":
            work_progress_transition_id = t["id"]
            break

    if not work_progress_transition_id:
        print(f"[ERROR] 'Waiting for customer' transition not found for {key}. Available transitions: {[t['name'] for t in transitions]}")
        return

    # Step 3: Transition the ticket
    transition_payload = {
        "transition": {
            "id": work_progress_transition_id
        }
    }

    transition_response = requests.post(transitions_url, headers=headers, auth=auth, json=transition_payload)

    if transition_response.status_code != 204:
        print(f"[ERROR] Failed to transition {key} to 'Waiting for customer': {transition_response.text}", file=sys.stderr)
    else:
        print(f"[INFO] Ticket {key} transitioned to 'Waiting for customer'.")

    # Step 4: Get current user's account ID
    myself_url = "https://hpcrcf.atlassian.net/rest/api/3/myself"
    user_response = requests.get(myself_url, headers=headers, auth=auth)

    if user_response.status_code != 200:
        print(f"[ERROR] Failed to retrieve current user info: {user_response.text}", file=sys.stderr)
        return

    account_id = user_response.json().get("accountId")

    # Step 5: Assign the ticket to the current user
    assignee_url = f"https://hpcrcf.atlassian.net/rest/api/3/issue/{key}/assignee"
    assignee_payload = {
        "accountId": account_id
    }

    assign_response = requests.put(assignee_url, headers=headers, auth=auth, json=assignee_payload)

    if assign_response.status_code != 204:
        print(f"[ERROR] Failed to assign {key} to {email}: {assign_response.text}", file=sys.stderr)
    else:
        print(f"[INFO] Ticket {key} assigned to {email}.")

def send_account_requests(extra_var_fileds):
    """
    Automates account request processing by calling an Ansible playbook with issue data.
    After each successful run, updates the corresponding JIRA ticket status and assignment.

    Args:
        extra_var_fileds (list): A list of lists, each containing:
                                 [first_name, last_name, pirg, jira_key]
    """
    current_dir = pathlib.Path(__file__).parent.resolve()
    playbook = current_dir / ".."  # Navigate to parent directory

    for issue_vars in extra_var_fileds:
        first, last, pirg, key = issue_vars
        result = subprocess.run(
            f"cd {playbook}  && ansible-playbook playbooks/slurm/account_request.yml "
            f"--extra-vars 'first={first} last={last} pirg={pirg} skip_confirmation=true'",
            shell=True
        )
        if result.returncode == 0:
            change_ticket_status(key)
    return

if __name__ == "__main__":
    # Run this script to fetch account requests, process them, and update JIRA
    send_account_requests(pull_filtered_tickets())
