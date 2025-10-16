# CFD Project - DS1

Data science project analyzing DC Inbox data.

## Setup

### Prerequisites

- Python 3.x with pandas
- Git LFS (for large data files)

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/marobinette/cfd-project-ds1.git
   cd cfd-project-ds1
   ```

2. **Install Git LFS:**
   
   **macOS:**
   ```bash
   brew install git-lfs
   git lfs install
   ```
      
   **Windows:**
   - Download and install from [git-lfs.github.com](https://git-lfs.github.com)
   - Run: `git lfs install`

3. **Pull the large data files:**
   ```bash
   git lfs pull
   ```

4. **Install Python dependencies:**
   ```bash
   pip install pandas jupyter
   ```

## Project Structure

```
cfd-project-ds1/
├── data/
│   ├── dcInbox/          # DC Inbox CSV data files (tracked with Git LFS)
│   └── finance/
├── notebooks/
│   └── dcInbox.ipynb     # Main analysis notebook
└── README.md
```

## Usage

### NLP on DC Inbox data set

1. Start Jupyter Notebook:
   ```bash
   jupyter notebook
   ```

2. Open `notebooks/dcInbox.ipynb` to run the analysis

### Merging DC Inbox & FEC data sets

1. Start Jupyter Notebook:
   ```bash
   jupyter notebook
   ```

2. Open `notebooks/DCInbox_ETL.ipynb` and run it to create the DC Inbox data set for matching to FEC data
   
3. Open `notebooks/Open_Secrets_ETL.jpynb` and run it to create the FEC data set for matching to DC Inbox data

4. Open `python/fuzzy_string_matching.py` and run it to match the DC Inbox data to the FEC data
   
5. Open notebooks/Matched_Politicians_Dataset_Creation.jpynb to create the matched politicians data set for analysis

### Exploratory Data Analysis

1. Open `Matched_Politicians_EDA.jpynb` to run the analysis on the matched politicians data set
   
## Working with Large Files

This project uses **Git LFS** to manage large CSV files. When adding new large files:

### Adding Large Data Files

```bash
# CSV files in data/dcInbox/ are automatically tracked with LFS
git add data/dcInbox/your_new_file.csv
git commit -m "Add new data file"
git push origin main
```

### Tracking Large Files in Other Directories

If you need to track large files in other locations:

```bash
# Track a new pattern (e.g., all CSVs in data/finance/)
git lfs track "data/finance/*.csv"

# Add the .gitattributes file
git add .gitattributes

# Then add your files normally
git add data/finance/*.csv
git commit -m "Add finance data files"
git push origin main
```

### Check LFS Status

```bash
# See which files are tracked by LFS
git lfs ls-files

# See LFS tracking patterns
cat .gitattributes
```

## Data

The project analyzes DC Inbox data from multiple export files covering various time periods.

## Notes

- Large CSV files (>50MB) are stored using Git LFS
- Make sure to run `git lfs pull` after cloning to download all data files
- If you get LFS-related errors, ensure Git LFS is installed with `git lfs install`

