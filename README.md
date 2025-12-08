# CFD Project - DS1

This repository tracks exploratory data analysis on two data sets: DC Inbox and OpenSecrets. DC Inbox contains email newsletter content sent by members of congress to their constituents, while OpenSecrets contains campaign fundraising information in the form of individual contributions.

## Project Structure

```
cfd-project-ds1/
├── data/
│   ├── dcInbox/ # DC Inbox CSV data files (tracked with Git LFS)
│   └── emotion_scores/
│   └── fec/
│   └── matched/
├── emails/ # Selected emails converted to HTML for readability
├── matching/ # Matching candidates from emails to contributions
├── notebooks/ # Data analysis
└── README.md
```

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

## Notes

- Large CSV files (>50MB) are stored using Git LFS
- Make sure to run `git lfs pull` after cloning to download all data files
- If you get LFS-related errors, ensure Git LFS is installed with `git lfs install`

