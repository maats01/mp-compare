import requests
import pandas as pd
import sys
import re
from time import sleep, perf_counter
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

def get_team_names(match_name):
    title = re.search(r"([a-zA-Z0-9]+): \(([^)]+)\) (VS|vs) \(([^)]+)\)", match_name)
    red_team_name = title.group(2)
    blue_team_name = title.group(4)

    return red_team_name, blue_team_name

def get_mappool_df(spreadsheet_link):
    rule = r"https://docs\.google\.com/spreadsheets/d/([a-zA-Z0-9-_]+)(?:.*[?&]gid=([0-9]+))?"
    spreadsheet = re.search(rule, spreadsheet_link)

    spreadsheet_id = spreadsheet.group(1)
    sheet_id = spreadsheet.group(2)
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/gviz/tq?tqx=out:csv&gid={sheet_id}"

    mappool_df = pd.read_csv(url, header=None).replace(r'^[^a-zA-Z0-9]+$', None, regex=True)
    mappool_df = mappool_df.replace(r'^\s*$', None, regex=True)
    mappool_df = mappool_df.dropna(how="all", axis=1).dropna(how="all", axis=0)
    
    return mappool_df

def get_slot_and_beatmapid_columns(df, beatmaps):
    slot_col = None
    map_id_col = None
    slot_pattern = r"(NM|HD|HR|DT|TB)"

    beatmaps = set(str(beatmap_id) for beatmap_id in beatmaps)

    for col in df.columns:
        col_data = df[col]

        if slot_col is None:
            if col_data.astype(str).str.match(slot_pattern, na=False).any():
                slot_col = col

        if map_id_col is None:
            if col_data.astype(str).isin(beatmaps).any():
                map_id_col = col

        if slot_col is not None and map_id_col is not None:
            break
    
    return slot_col, map_id_col

access_token = get_access_token()

spreadsheet_link = input("URL of the mappool (mappool sheet from the tourney's spreadsheet): ")

n_matches = int(input("Number of matches to compare: "))

mp_links = {}
for i in range(1, n_matches + 1):
    mp_id = int(input(f"{i} - match id: "))
    mp_links[mp_id] = {
        "Blue": "",
        "Red": ""
    }

start_total = perf_counter()

matches = []

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {access_token}"
}

start_api = perf_counter()
for id in mp_links.keys():
    print("="*20)
    print(f"Match ID: {id}")
    full_match_data = get_full_match_data(id, headers)

    if full_match_data:
        red_team_name, blue_team_name = get_team_names(full_match_data["match"]["name"])
        mp_links[id]["Red"] = red_team_name
        mp_links[id]["Blue"] = blue_team_name

        matches.append(full_match_data)

end_api = perf_counter()

dfs_team_scores = []
individual_scores_per_map = {}
individual_scores_count = 0
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
                individual_scores_count += 1
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

# tenho que fazer algumas verificações aqui, caso o request da spreadsheet falhe
mappool_df = get_mappool_df(spreadsheet_link)

slot_mapping = {beatmap_id: "" for beatmap_id in individual_scores_per_map}

slot_column, map_id_column = get_slot_and_beatmapid_columns(mappool_df, slot_mapping.keys())

for beatmap_id in slot_mapping:
    slot = mappool_df.loc[mappool_df[map_id_column] == str(beatmap_id), slot_column].values[0]
    slot_mapping[beatmap_id] = slot

final_df = pd.concat(dfs_team_scores, axis=1)
final_df = final_df.T.groupby(final_df.columns).mean().T

individual_mean_scores_per_map = {
    map_id: {user: mean(scores) for user, scores in users_scores.items()}
    for map_id, users_scores in individual_scores_per_map.items()
}

individual_scores_df = pd.DataFrame(individual_mean_scores_per_map)

mean_per_map = individual_scores_df.mean()
std_per_map = individual_scores_df.std()
z_scores_df = (individual_scores_df - mean_per_map) / std_per_map

z_scores_df = z_scores_df.round(2)
z_scores_df["z_sum"] = z_scores_df.sum(axis=1)
z_scores_df.sort_values(by="z_sum", ascending=False, inplace=True)

base_beatmap_url = "https://osu.ppy.sh/beatmaps/"
final_df.index = [
    f'=HYPERLINK("{base_beatmap_url}{idx}", "{slot_mapping[idx]}")'
    for idx in final_df.index
]
individual_scores_df.columns = [
    f'=HYPERLINK("{base_beatmap_url}{col}", "{slot_mapping[col]}")'
    for col in individual_scores_df.columns
]
z_scores_df.columns = [
    f'=HYPERLINK("{base_beatmap_url}{col}", "{slot_mapping[col]}")' if str(col).isdigit() else col
    for col in z_scores_df.columns
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
        z_scores_df.to_excel(writer, sheet_name="Z-Scores", index=True)

        workbook = writer.book
        integer_format = workbook.add_format({"num_format": "#,##0"})

        team_scores_sheet = writer.sheets["Team Scores"]
        individual_scores_sheet = writer.sheets["Individual Scores"]
        z_scores_sheet = writer.sheets["Z-Scores"]
        
        individual_scores_sheet.set_column(0, 0, 20)
        z_scores_sheet.set_column(0, 0, 20)

        for row_num, value in enumerate(final_df.index):
            actual_row = row_num + 1

            if "NM" in value:
                color = "#a4c2f4"
            elif "HD" in value:
                color = "#f9cb9c"
            elif "HR" in value:
                color = "#ea9999"
            elif "DT" in value:
                color = "#b4a7d6"
            elif "FM" in value:
                color = "#b6d7a8"
            else:
                color = "#a2c4c9"

            format = workbook.add_format({"bg_color": color, "bold": True, "font_color": "#ffffff"})
            team_scores_sheet.write(actual_row, 0, value, format)

        for col_num, value in enumerate(individual_scores_df.columns):
            actual_col = col_num + 1

            if "NM" in value:
                color = "#a4c2f4"
            elif "HD" in value:
                color = "#f9cb9c"
            elif "HR" in value:
                color = "#ea9999"
            elif "DT" in value:
                color = "#b4a7d6"
            elif "FM" in value:
                color = "#b6d7a8"
            else:
                color = "#a2c4c9"
            
            format = workbook.add_format({"bg_color": color, "bold": True, "font_color": "#ffffff"})
            individual_scores_sheet.write(0, actual_col, value, format)

        for col_num, value in enumerate(z_scores_df.columns):
            actual_col = col_num + 1

            if "NM" in value:
                color = "#a4c2f4"
            elif "HD" in value:
                color = "#f9cb9c"
            elif "HR" in value:
                color = "#ea9999"
            elif "DT" in value:
                color = "#b4a7d6"
            elif "FM" in value:
                color = "#b6d7a8"
            elif "TB" in value:
                color = "#a2c4c9"
            else:
                color = "#000000"
            
            format = workbook.add_format({"bg_color": color, "bold": True, "font_color": "#ffffff"})
            z_scores_sheet.write(0, actual_col, value, format)

        for i, col_name in enumerate(final_df.columns):
            col_letter = xl_col_to_name(i + 1)
            team_scores_sheet.set_column(f"{col_letter}:{col_letter}", 10, integer_format)

        for i, col_name in enumerate(individual_scores_df.columns):
            col_letter = xl_col_to_name(i + 1)
            individual_scores_sheet.set_column(f"{col_letter}:{col_letter}", 10, integer_format)

        print("Results sheet succesfully created!\n")

except FileNotFoundError:
    print("File path not found.")

except Exception as e:
    print(f"A unexpected problem happened while generating the excel file: {e}")

end_total = perf_counter()

api_time = end_api - start_api
processing_time = end_total - end_api
total_time = end_total - start_total

print("--- Performance Report ---")
print(f"Processed {individual_scores_count} scores from {len(matches)} matches.")
print(f"Time spent consuming osu!'s API: {api_time:.3f}s.")
print(f"Time spent processing the response: {processing_time:.3f}s.")
print(f"Total running time: {total_time:.3f}s.")