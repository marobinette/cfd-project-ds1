import csv
import os
import re
from datetime import datetime

# Set directories
work_dir = "C:/Users/hefla/Documents/Work/IPS/Area 990/Python/TheFuzz"
output_dir  = "C:/Users/hefla/Documents/Work/IPS/Area 990/Python/TheFuzz/output"

# Set file names
# input_file_name = "fuzzy_targets - c3 flows 2020.csv"
# input_file_name = "fuzzy_targets - c3 flows 2021.csv"
# input_file_name = "fuzzy_targets - c3 flows 2022.csv"
# input_file_name = "fuzzy_targets - c3 flows 2023.csv"
# input_file_name = "fuzzy_match_candidates - c3 flows part ii 2020.csv"
# input_file_name = "fuzzy_match_candidates - c3 flows part ii 2021.csv"
# input_file_name = "fuzzy_match_candidates - c3 flows part ii 2022.csv"
# input_file_name = "fuzzy_match_candidates - c3 flows part ii 2023.csv"
input_file_name = "fuzzy_match_candidates - c3 flows part ii with null EINs 2020.csv"
# input_file_name = "fuzzy_targets - c3 flows match improvement test.csv"
# input_file_name = "fuzzy_match_candidates_part_ii - c3 flows match improvement test.csv"
# input_file_name = "fuzzy_match_candidates_bmf - c3 flows match improvement test.csv"

# Set input file to preprocess
input_file  = os.path.join(work_dir, input_file_name)

# Set output file
base, ext = os.path.splitext(input_file_name)
timestamp   = datetime.now().strftime('%Y%m%d_%H%M%S')
output_file_name = f"{base}_prepped_{timestamp}.csv"
output_file = os.path.join(output_dir, output_file_name)

# Set list of stop words
stopwords = {
    "CORP", "CORPORATION", "INC", "INCORPORATED", "LLC", "LLP", "LP", "LTD",
    "&", "AND", "AS", "AT", "FOR", "OF", "ON", "THE"
}

# Set dictionary of standard abbreviations 
abbreviations = {
    "ASSN": "ASSOCIATION",
    "FDN": "FOUNDATION",
    "FND": "FUND"
}

# Preprocessing function
def preprocess_name(name: str) -> str:
    if not name:
        return ""
    # Set all names to uppercase
    name = name.upper()
    # remove integers surrounded by parentheses (e.g. "(1)")
    name = re.sub(r"\(\d+\)", " ", name)
    # Strip out everything from "DBA" or "C/O" onward
    name = re.sub(r"\b(DBA|C/O)\b.*", " ", name)
    # Strip out "XXX-XX-XXXX"
    name = re.sub(r"XXX-XX-XXXX", " ", name)
    # strip punctuation (replace with space so words don't merge) EXCEPT forward slashes
    # name = re.sub(r"[^A-Z0-9\s]", " ", name)
    name = re.sub(r"[^A-Z0-9\s/]", " ", name)
    # remove stopwords
    words = [w for w in name.split() if w not in stopwords]
    # standardize abbreviations
    words = [abbreviations.get(w, w) for w in words]
    # whitespace cleanup
    name = " ".join(words).strip()
    # print(name)
    return name

# Main block
with open(input_file, newline="", encoding="utf-8") as infile, \
     open(output_file, "w", newline="", encoding="utf-8") as outfile:

    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    # read header and add new column
    header = next(reader)
    header.append("prepped_name")
    writer.writerow(header)

    # process rows
    for row in reader:
        name = row[0]  # assumes first column is "name"
        prep_name = preprocess_name(name)
        row.append(prep_name)
        writer.writerow(row)

print(f"Done! Prepped file written to {output_file}")
