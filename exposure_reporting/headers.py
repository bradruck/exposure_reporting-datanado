# headers.py


def get_summary_headers():
    return ["Report Number", "Campaign Name", "Run Date", "Rows in Exposure File",
            "Total Impressions Served", "Insegment Impressions", "Impressions Matched to DLX HHID",
            "Exposed Unique HH", "Impressions in File", "Customer IDs in File",
            "Exposed Unique Customer IDs in File", "Exposed Unique HH in File",
            "Creative Count", "Placement Count", "Exposure Start Date", "Exposure End Date"]


def get_weekly_headers():
    return ["Week Number", "Start Date", "Count Impressions", "Avg Imps Per CustID"]


def get_duplicate_headers():
    return ["Report Number", "Campaign Name", "Two Duplicates (Unique CustIDs)", "Three", "Four", "Five", "Ten+", "% Unique CustIDs as Duplicates"]
