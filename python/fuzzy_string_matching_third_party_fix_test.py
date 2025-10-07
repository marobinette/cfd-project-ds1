# -------------------------------------------------------------------
# Process to match DC Inbox politicians to FEC Open Secrets
# candidates. Adapted from a general nonprofit matching process created 
# by Helen Flannery for the Institute for Policy Studies.
# -------------------------------------------------------------------

import csv
import os
import re
from datetime import datetime
from collections import defaultdict
from rapidfuzz import process, fuzz # Switch to RapidFuzz (accelerated)

# Set parameters
min_similarity_score = 75
match_on_attribute1     = True
match_on_attribute2     = True
match_on_attribute3     = True
fix_third_parties       = True
output_file_term        = "dcinbox_match_3party_test_2020"

# Define target names to skip
# skip_names = {
#     "COMMUNITY FOUNDATION",
#     "COMMUNITYS FOUNDATION",
#     "COMMUNITY FOUNDATIONS",
#     "NONE"
# }

# Define directories
# work_dir = "C:/Users/hefla/Documents/Work/IPS/Area 990/Python/TheFuzz"
work_dir = "C:/Users/hefla/GitHub/cfd-project-ds1/matching"
# output_dir  = "C:/Users/hefla/Documents/Work/IPS/Area 990/Python/TheFuzz/output"
# output_dir  = "C:/Users/hefla/GitHub/cfd-project-ds1/matching"
output_dir = "C:/Users/hefla/Documents/School/Classes/CSYS 5870/Class Project/Matching QC"
# log_dir  = "C:/Users/hefla/Documents/Work/IPS/Area 990/Python/TheFuzz/logs"
log_dir = "C:/Users/hefla/GitHub/cfd-project-ds1/matching"
third_party_fix_dir = "C:/Users/hefla/GitHub/cfd-project-ds1/data/dcinbox"

# Define input files
third_party_fixes_file = os.path.join(third_party_fix_dir, f"dcinbox_third_party_fixes.csv")
# targets_file = os.path.join(work_dir, f"fuzzy_targets.csv")
# targets_file = os.path.join(work_dir, f"fuzzy_targets - match_test.csv")
targets_file = os.path.join(work_dir, f"match_targets_2020_test.csv")
# match_candidates_file = os.path.join(work_dir, f"fuzzy_match_candidates.csv")
# match_candidates_file = os.path.join(work_dir, f"fuzzy_match_candidates - match_test.csv")
match_candidates_file = os.path.join(work_dir, f"match_candidates_2020_test.csv")
# match_candidates_file = os.path.join(work_dir, f"fuzzy_match_candidates - c3 flows part ii with null EINs 2023_prepped.csv")

# Define output directory and output file
os.makedirs(output_dir, exist_ok=True)
timestamp   = datetime.now().strftime('%Y%m%d_%H%M%S')
output_file = os.path.join(output_dir, f"match_output_{min_similarity_score}_{output_file_term}_{timestamp}.csv")
unmatched_file = os.path.join(output_dir, f"unmatched_records_{min_similarity_score}_{output_file_term}_{timestamp}.csv")
log_file = os.path.join(log_dir, f"match_log_{min_similarity_score}_{output_file_term}_{timestamp}.csv")

# Set counters for log file
num_targets = 0
total_matches = 0
skipped_records = 0
blank_records = 0
bad_name_records = 0
unmatched_due_to_attributes = 0

# Initialize a list for the unmatched targets
unmatched_targets = []

# Load in third_party name fixes file 
if fix_third_parties:
    third_party_fixes = []
    third_party_fix_cids = []
    with open(third_party_fixes_file, newline='', encoding='latin1') as rf:
        rdr = csv.reader(rf)
        next(rdr)
        for row in rdr:
            # if search_type == "attribute1":
            third_party_fixes.append({
                "cid":              row[0],
                "name":             row[1],
                "dcinbox_party":    row[2],
                "change":           row[3],
                "change_to":        row[4],
            })
            third_party_fix_cids.append(
                row[0]
            )
            # else:
            #     raise ValueError("unsupported type")
        
# Load in target data (the list of base target items you want to match TO)
targets = []
with open(targets_file, newline='', encoding='latin1') as tf:
    rdr = csv.reader(tf)
    next(rdr)
    for row in rdr:
        # name = row[0].upper()
        name = (row[0] or "").strip().upper()
        # Skip if name is blank
        if not name:
            blank_records += 1
            continue        
        # Don't add skip_names names to the search list
        # if name in skip_names:
        #     skipped_records += 1
        #     continue       
        # Don't add any names that contain the following words to the search list
        if any(bad in name for bad in ["VARIOUS", "N/A"]):
            bad_name_records += 1
            continue
        # Loop through third_party fixes table to check alternate names, if they exist
        if fix_third_parties:
            if row[0] in third_party_fix_cids:
                for third_party in third_party_fixes:
                    # Fix the third_party's party if it appears in the third_party_fixes file with a YES in the CHANGE field
                    if row[0] == third_party['cid'] and third_party['change'] == 'YES':
                        name = third_party['change_to']
                        # print("Fixed name: ", name)
                    # Add additional row if the third_party has an alternate name to search on (that is not NONE)
                    # if row[0] == third_party['id'] and third_party['alternate_name'] != 'NONE':
                        # # print("Appending alternate name: ", third_party['alternate_name'])
                        # alt_name = third_party['alternate_name']
                        # targets.append({
                        #     "id":           row[0],
                        #     "name":         alt_name,
                        #     "attribute1":   row[2],
                        #     "attribute2":   row[3]
                        # })
                        # # print(third_party)
        # print("Appending: ", name)
        targets.append({
            "name":             name,
            "id":               row[1],
            "match_attribute1": row[2],
            "match_attribute2": row[3],
            "match_attribute3": row[4]
        })
        num_targets += 1

# Load in match candidate data (the pool of potential match candidates you want to match FROM)
match_candidates = []
seen_candidates = {}

# Function to see if match_candidate has a real, non-empty ID 
def has_real_id(idval):
    # Return True for a non-empty, non-placeholder ID.
    # Treats empty strings and literal values like 'NULL', 'NONE', 'N/A' as missing."
    if not idval:
        return False
    s = idval.strip().upper()
    return s not in ("", "NULL", "NONE", "N/A")

with open(match_candidates_file, newline='', encoding='latin1') as rf:
    rdr = csv.reader(rf)
    next(rdr)
    for row in rdr:
        # rname = row[0]
        rname = (row[0] or "").strip().upper()
        # Skip if name is blank
        if not rname:
            continue
        # Don't add skip_names names to the search list
        # if rname in skip_names:
        #     continue 
        # if the unique combination of candidate name and matching attribute(s) 
        # doesn't already exist in the comparison list, then add it
        # By building a dynamic candidate_combo list of fields to match 
        candidate_combo_parts = [row[0]] # initialize candidate_combo components list with the candidate itself
        if match_on_attribute1:
            candidate_combo_parts.append(row[2])
        if match_on_attribute2:
            candidate_combo_parts.append(row[3])
        if match_on_attribute3:
            candidate_combo_parts.append(row[4])
        # Build candidate_combo list for matching
        candidate_combo = tuple(candidate_combo_parts)
        # print(candidate_combo)

        current_has_id = has_real_id(row[1])

        candidate_dict = {
            "name":                 row[0],
            "id":                   row[1],
            "match_attribute1":     row[2],
            "match_attribute2":     row[3],
            "match_attribute3":     row[4],
            "append_attribute1":    row[5],
            "append_attribute2":    row[6],
            "append_attribute3":    row[7]
        }

        # Make existing variable take value of the candidate_combo, if there is one
        existing = seen_candidates.get(candidate_combo)
        # print("existing: ", existing)

        if existing is None:
            # If this is the first time we see this combo, then store it in the dictionary
            seen_candidates[candidate_combo] = candidate_dict
        else:
            existing_has_id = has_real_id(existing.get("id"))
            # Prefer rows that have a real ID: only replace if current has ID and existing doesn't.
            if not existing_has_id and current_has_id:
                seen_candidates[candidate_combo] = candidate_dict

        # if candidate_combo in seen_candidates:
        #     continue
        # seen_candidates.add(candidate_combo)
        # match_candidates.append({
        #     "name":                 row[0],
        #     "id":                   row[1],
        #     "match_attribute1":     row[2],
        #     "match_attribute2":     row[3],
        #     "match_attribute3":     row[4],
        #     "append_attribute1":    row[5],
        #     "append_attribute2":    row[6],
        #     "append_attribute3":    row[7]
        # })
        # print(row[0])

# Rehydrate match_candidates from the dict (preserves one entry per combo; order may differ)
match_candidates = list(seen_candidates.values())

# Index the search targets by name (for default searches)
target_names   = [t["name"] for t in targets]
target_lookup  = { t["name"]: t for t in targets }

# Index the match candidates by name (for default searches)
candidate_names   = [c["name"] for c in match_candidates]
candidate_lookup  = { c["name"]: c for c in match_candidates }

# Index the search targets depending on which match attributes you're using
targets_index = defaultdict(list)
for t in targets:
    match_components = []
    if match_on_attribute1:
        match_components.append(t["match_attribute1"])
    if match_on_attribute2:
        match_components.append(t["match_attribute2"])
    if match_on_attribute3:
        match_components.append(t["match_attribute3"])
    # default to indexing on a constant if no match attributes are chosen
    match_attributes = tuple(match_components) if match_components else ("ALL",)
    targets_index[match_attributes].append(t["name"])        

# Index the match candidates depending on which match attributes you're using
candidates_index = defaultdict(list)
for c in match_candidates:
    match_components = []
    if match_on_attribute1:
        match_components.append(c["match_attribute1"])
    if match_on_attribute2:
        match_components.append(c["match_attribute2"])
    if match_on_attribute3:
        match_components.append(c["match_attribute3"])
    # default to indexing on a constant if no match attributes are chosen
    match_attributes = tuple(match_components) if match_components else ("ALL",)
    candidates_index[match_attributes].append(c["name"])        

# Initialize the output rows variable and add a header row to it
output_rows = []
header = [
    'target_id', 'target_name', 
    'matched_candidate_id', 'matched_candidate_name',
    'matched_attribute1', 'matched_attribute2', 'matched_attribute3', 
    'append_attribute1', 'append_attribute2', 'append_attribute3', 
    'similarity_score', 'matched_scorer'
]
output_rows.append(header)

# Iterate through the grant recipients and match them to the target third_partys
for t in targets:
    
    target_name  = t["name"]
    target_match_attribute1 = t["match_attribute1"]
    best_match = None
    best_score = -1
    best_name = None
    matched_scorer = None
    matched_this_target = False
    
    # If the name is an 8- or 9-digit integer, use that as the ID and bypass matching
    if re.fullmatch(r"\d{8,9}", target_name):
        # Directly output the row without fuzzy matching
        output_rows.append([
            t["id"],             # target_id
            t["name"],           # target_name
            t["name"],           # matched_candidate_id (use numeric name)
            "",                  # matched_candidate_name
            t["match_attribute1"],
            t["match_attribute2"],
            t["match_attribute3"],
            "", "", "",          # append_attribute1/2/3 left blank
            "100",               # similarity_score left blank
            "Name is ID"         # matched_scorer left blank
        ])
        continue     

    # define scorers in priority order
    scorers = [
        ("WRatio", fuzz.WRatio),
        ("partial_ratio", fuzz.partial_ratio),
        ("token_set_ratio", fuzz.token_set_ratio)
    ]    

    # pick candidate pool / build lookup key for this target
    match_components = []
    if match_on_attribute1:
        match_components.append(t["match_attribute1"])
    if match_on_attribute2:
        match_components.append(t["match_attribute2"])
    if match_on_attribute3:
        match_components.append(t["match_attribute3"])
    match_attributes = tuple(match_components) if match_components else ("ALL",)
    match_pool = candidates_index.get(match_attributes, [])
    if not match_pool:
        unmatched_due_to_attributes += 1
        # unmatched_targets.append(t)
        continue

    for scorer_name, scorer in scorers:
        # fuzzy match with score cutoff
        candidate = process.extractOne(
            target_name,
            match_pool,
            scorer = scorer,
            score_cutoff = min_similarity_score
        )
        if candidate:
            cand_name, cand_score, _ = candidate
            # keep the candidate with the highest score across scorers
            if cand_score > best_score:
                best_name = cand_name
                best_score = cand_score
                best_match = candidate
                matched_scorer = scorer_name

    if not best_match:
        continue

    best_name, best_score, _ = best_match  # name, score, index

    print(f"Found match: '{target_name}' -> '{best_name}' (score = {best_score} using {matched_scorer})")

    cand = candidate_lookup[best_name]

    # assemble row
    output_rows.append([
        t["id"],
        t["name"],
        cand["id"],
        best_name,
        t["match_attribute1"],
        t["match_attribute2"],
        t["match_attribute3"],
        cand["append_attribute1"],
        cand["append_attribute2"],
        cand["append_attribute3"],
        best_score,
        matched_scorer
    ])

    total_matches += 1
    matched_this_target = True

    if not matched_this_target:
        unmatched_targets.append(t)

# Write output to file
with open(output_file, 'w', newline='', encoding='latin1') as wf:
    writer = csv.writer(wf)
    writer.writerows(output_rows)

with open(unmatched_file, "w", newline="", encoding="latin1") as unf:
    writer = csv.DictWriter(unf, fieldnames=["name", "id", "match_attribute1", "match_attribute2", "match_attribute3"])
    writer.writeheader()
    for row in unmatched_targets:
        writer.writerow(row)

# Write log file
with open(log_file, "w", encoding = "latin1") as logf:
    logf.write(f"Number of targets: {num_targets}\n")
    logf.write(f"Total matches: {total_matches}\n\n")
    # logf.write(f"Skipped records (due to skip_names): {skipped_records}\n")
    logf.write(f"Skipped records (due to blank_records): {blank_records}\n")
    logf.write(f"Skipped records (due to bad_names): {bad_name_records}\n")
    logf.write(f"\nTotal unmatched: {num_targets - total_matches}\n")
    logf.write(f"Unmatched (because no candidates had the right match attributes): {unmatched_due_to_attributes}\n")
    logf.write(f"Other targets not matched: {num_targets - (unmatched_due_to_attributes + total_matches)}\n")

print(f"Done! {len(output_rows)-1} matches written to {output_file}")
