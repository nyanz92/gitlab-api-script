import requests
import os
import datetime
#from dotenv import load_dotenv

# Load environment variables from .env file
#load_dotenv()

# --- Configuration ---
GITLAB_URL = os.getenv("GITLAB_URL")
PRIVATE_TOKEN = os.getenv("GITLAB_PRIVATE_TOKEN")

# API Endpoints
USERS_API_URL = f"{GITLAB_URL}/api/v4/users"
# Note: The API is paginated, so we use per_page=100 (max) to minimize requests
EVENTS_PER_PAGE = 100 
USERS_PER_PAGE = 100

# Headers for authentication
HEADERS = {
    "Private-Token": PRIVATE_TOKEN
}

# --- Date Calculation ---
MONTHS_TO_GO_BACK = 5
# Calculate the 'after' date (5 months ago)
# We use the current date and subtract 5 months (approx 150 days)
# Using `relativedelta` from dateutil is more precise for months, but we use
# datetime.timedelta for simplicity since it's built-in.
# NOTE: For precise 5-month calculation, consider installing and using `python-dateutil`.
end_date = datetime.date.today() + datetime.timedelta(days=1) # Get events up to tomorrow
start_date = end_date - datetime.timedelta(days=MONTHS_TO_GO_BACK * 30) 

# Convert to ISO 8601 format required by the API
AFTER_DATE = start_date.strftime('%Y-%m-%d')
BEFORE_DATE = end_date.strftime('%Y-%m-%d')

print(f"Counting 'pushed to' events from {AFTER_DATE} to {BEFORE_DATE}...")

# --- Helper Functions ---

def get_all_users():
    """Retrieves all active user IDs and usernames from GitLab, handling pagination."""
    all_users = []
    page = 1
    
    while True:
        params = {
            'per_page': USERS_PER_PAGE,
            'page': page,
            'active': True # Only count events for active users
        }
        
        try:
            response = requests.get(USERS_API_URL, headers=HEADERS, params=params)
            response.raise_for_status()
            
            users = response.json()
            if not users:
                break # No more users
            
            for user in users:
                all_users.append({
                    'id': user.get('id'),
                    'username': user.get('username')
                })
            
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"Error fetching users: {e}")
            return None
    
    print(f"Found {len(all_users)} active users.")
    return all_users

def get_user_push_count(user_id, username):
    """Counts 'pushed to' events for a single user within the time window."""
    push_count = 0
    page = 1
    
    while True:
        # API endpoint for a specific user's events
        USER_EVENTS_URL = f"{GITLAB_URL}/api/v4/users/{user_id}/events"
        params = {
            'action': 'pushed',        # Filter by pushed events
            'after': AFTER_DATE,
            'before': BEFORE_DATE,
            'per_page': EVENTS_PER_PAGE,
            'page': page
        }
        
        try:
            response = requests.get(USER_EVENTS_URL, headers=HEADERS, params=params)
            response.raise_for_status()
            
            events = response.json()
            if not events:
                break # No more events
            
            # The 'action' filter only works on some types. 
            # We explicitly check the action_name to be certain.
            for event in events:
                if event.get('action_name') == 'pushed to':
                    push_count += 1
            
            page += 1
            
            # Optimization: If the number of events returned is less than the max per page,
            # we assume we've hit the end and break early.
            if len(events) < EVENTS_PER_PAGE:
                break

        except requests.exceptions.RequestException as e:
            print(f"Error fetching events for user {username} (ID: {user_id}): {e}")
            break
            
    return push_count

# --- Main Logic ---

if __name__ == "__main__":
    if not GITLAB_URL or not PRIVATE_TOKEN:
        print("Error: GITLAB_URL or GITLAB_PRIVATE_TOKEN not found in .env file.")
    else:
        user_list = get_all_users()
        
        if user_list:
            results = {}
            total_pushes = 0
            
            print("-" * 50)
            print("Processing events for each user...")
            
            for user in user_list:
                user_id = user['id']
                username = user['username']
                
                # Fetch and count push events
                count = get_user_push_count(user_id, username)
                
                # Store only users who have push events
                if count > 0:
                    results[username] = count
                    total_pushes += count
                    print(f"-> {username:<20}: {count} pushes")

            # --- Output Results ---
            print("\n" + "=" * 50)
            print(f"✅ GitLab User Push Event Summary ({MONTHS_TO_GO_BACK} Months)")
            print("=" * 50)
            
            # Sort the results by push count (highest first)
            sorted_results = sorted(results.items(), key=lambda item: item[1], reverse=True)
            
            for username, count in sorted_results:
                print(f"{username:<25} {count}")
                
            print("-" * 50)
            print(f"Total Unique Contributors: {len(results)}")
            print(f"Total Push Events:         {total_pushes}")
            print("=" * 50)