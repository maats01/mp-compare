import requests
import pandas as pd
import sys
from time import sleep
from xlsxwriter.utility import xl_col_to_name
from keyring import get_credential, set_password, errors
from pathlib import Path
from statistics import mean
from datetime import datetime

SERVICE_NAME = "mp_compare"
user_credential = get_credential(SERVICE_NAME, None)

if not user_credential:
    print("OAuth credential not found. The application needs it to consume osu's api.")
    osu_client_id = input("Osu client ID: ")
    osu_client_secret = input("Osu client secret: ")
    try:
        set_password(SERVICE_NAME, osu_client_id, osu_client_secret)
    except errors.PasswordSetError:
        print("Failed to store a new OAuth credential.")

user_credential = get_credential(SERVICE_NAME, None)

CLIENT_ID = user_credential.username
CLIENT_SECRET = user_credential.password

def get_access_token():
    auth_url = "https://osu.ppy.sh/oauth/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "public"
    }

    response = requests.post(auth_url, data=data)
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        raise Exception(F"Error to obtain token: {response.status_code} - {response.text}")

def get_full_match_data(match_id, headers):
    all_events = []
    all_users = {}
    first_event_id = None
    match_metadata = {}
    
    while True:
        params = {"before": first_event_id} if first_event_id else {}
        response = requests.get(f"https://osu.ppy.sh/api/v2/matches/{match_id}", headers=headers, params=params)
        
        if response.status_code != 200:
            break
            
        data = response.json()

        if not match_metadata:
            match_metadata = data.copy()
            match_metadata.pop("events")
            match_metadata.pop("users") 
            
        novos_eventos = data.get("events", [])
        if not novos_eventos:
            break

        for user in data.get("users", []):
            all_users[user["id"]] = user["username"]

        all_events.extend(novos_eventos)
        
        first_event_id = novos_eventos[0]["id"]
        
        print(f"Collected {len(all_events)} events... (Current ID: {first_event_id})")
        sleep(0.5)

    all_events.reverse()
    match_metadata["events"] = all_events
    match_metadata["user_map"] = all_users
    return match_metadata

access_token = get_access_token()

n_matches = int(input("Number of matches to compare: "))
mp_links = {}

for i in range(1, n_matches + 1):
    print("="*20)
    link = int(input(f"{i} - match id: "))
    blue_team_name = input(f"Blue team name: ")
    red_team_name = input(f"Red team name: ")
    mp_links[link] = {
        "Blue": blue_team_name,
        "Red": red_team_name
    }

matches = []

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {access_token}"
}

for id in mp_links.keys():
    print("="*20)
    print(f"Match ID: {id}")
    full_match_data = get_full_match_data(id, headers)

    if full_match_data:
        matches.append(full_match_data)

dfs_team_scores = []
individual_scores_per_map = {}
for match in matches:
    beatmaps = []
    blue_team_scores = []
    red_team_scores = []
    match_id = match["match"]["id"]

    for event in match["events"]:
        if "game" in event:
            game = event["game"]
            beatmap_id = game["beatmap_id"]
            
            if beatmap_id not in individual_scores_per_map:
                individual_scores_per_map[beatmap_id] = {}
            
            beatmaps.append(beatmap_id)

            total_score_red = 0
            total_score_blue = 0

            for score in game["scores"]:
                total_score = score["score"]
                user_id = score["user_id"]
                username = match["user_map"][user_id]

                user_score = individual_scores_per_map.get(beatmap_id, {}).get(username)

                if not user_score:
                    individual_scores_per_map[beatmap_id][username] = [total_score]
                else:
                    individual_scores_per_map[beatmap_id][username].append(total_score)

                team = score["match"]["team"]
                
                if team == "red":
                    total_score_red += total_score
                else:
                    total_score_blue += total_score
            
            blue_team_scores.append(total_score_blue)
            red_team_scores.append(total_score_red)

    data = {
        "beatmap_id": beatmaps,
        mp_links[match_id]["Blue"]: blue_team_scores,
        mp_links[match_id]["Red"]: red_team_scores
    }

    df = pd.DataFrame(data)
    df = df.set_index("beatmap_id").groupby("beatmap_id").max()
    dfs_team_scores.append(df)

final_df = pd.concat(dfs_team_scores, axis=1)

individual_mean_scores_per_map = {
    map_id: {user: mean(scores) for user, scores in users_scores.items()}
    for map_id, users_scores in individual_scores_per_map.items()
}

individual_scores_df = pd.DataFrame(individual_mean_scores_per_map)

mean_per_map = individual_scores_df.mean()
std_per_map = individual_scores_df.std()
df_z_scores = (individual_scores_df - mean_per_map) / std_per_map

df_z_scores = df_z_scores.round(2)
df_z_scores["z_sum"] = df_z_scores.sum(axis=1)
df_z_scores.sort_values(by="z_sum", ascending=False, inplace=True)

base_beatmap_url = "https://osu.ppy.sh/beatmaps/"

final_df.index = [
    f'=HYPERLINK("{base_beatmap_url}{idx}", "{idx}")'
    for idx in final_df.index
]
individual_scores_df.columns = [
    f'=HYPERLINK("{base_beatmap_url}{col}", "{col}")'
    for col in individual_scores_df.columns
]
df_z_scores.columns = [
    f'=HYPERLINK("{base_beatmap_url}{col}", "{col}")' if str(col).isdigit() else col
    for col in df_z_scores.columns
]

if getattr(sys, 'frozen', False):
    base_path = Path(sys.executable).parent
else:
    base_path = Path(__file__).parent

tourney_name = matches[0]["match"]["name"].split(":")[0]
time_label = datetime.now().strftime("%Y%m%d_%H%M")

output_dir = base_path / "results"
output_file = output_dir / f"{tourney_name}_{time_label}.xlsx"
output_dir.mkdir(parents=True, exist_ok=True)

try:
    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        final_df.to_excel(writer, sheet_name="Team Scores", index=True)
        individual_scores_df.to_excel(writer, sheet_name="Individual Scores", index=True)
        df_z_scores.to_excel(writer, sheet_name="Z-Scores", index=True)

        workbook = writer.book
        integer_format = workbook.add_format({"num_format": "#,##0"})
        header_format = workbook.add_format({
            "bold": False,
            "font_color": "blue",
            "underline": 1,
            "num_format": "@",
            "align": "left"
        })

        team_scores_sheet = writer.sheets["Team Scores"]
        individual_scores_sheet = writer.sheets["Individual Scores"]
        z_scores_sheet = writer.sheets["Z-Scores"]
        
        individual_scores_sheet.set_column(0, 0, 20)
        z_scores_sheet.set_column(0, 0, 20)
        individual_scores_sheet.set_row(0, None, header_format)

        for i, col_name in enumerate(final_df.columns):
            col_letter = xl_col_to_name(i + 1)
            team_scores_sheet.set_column(f"{col_letter}:{col_letter}", 10, integer_format)

        for i, col_name in enumerate(individual_scores_df.columns):
            col_letter = xl_col_to_name(i + 1)
            individual_scores_sheet.set_column(f"{col_letter}:{col_letter}", 10, integer_format)
        
        print("Results sheet succesfully created!")

except FileNotFoundError:
    print("File path not found.")

except Exception as e:
    print(f"A unexpected problem happened while generating the excel file: {e}")