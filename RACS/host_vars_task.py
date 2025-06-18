import csv
import os
import subprocess
from pathlib import Path
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
yaml = YAML()
yaml.preserve_quotes = True
yaml.explicit_start = True
yaml.indent(mapping=2, sequence=2, offset=0)


def update_eos_dictionary(csv_file_path, eos_dict):
    """
    Update eos_dict with service tags and EOS dates from a given CSV file.
    Dynamically detects the header row even if metadata is present above it.
    """
    with open(csv_file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Find the valid header line
    header_line_index = -1
    for i, line in enumerate(lines):
        cols = [c.strip() for c in line.split(',')]
        if ("Svc Tag" in cols or "Tag" in cols) and ("Hardware/Primary End Date" in cols or "Existing End Date" in cols):
            header_line_index = i
            break

    if header_line_index == -1:
        print(f"Skipping {csv_file_path}: No valid header found.")
        return eos_dict

    # Read from header onward
    data_lines = lines[header_line_index:]
    reader = csv.DictReader(data_lines)
    headers = reader.fieldnames

    tag_key = "Svc Tag" if "Svc Tag" in headers else "Tag"
    eos_key = "Hardware/Primary End Date" if "Hardware/Primary End Date" in headers else "Existing End Date"

    for row in reader:
        tag = row.get(tag_key, "").strip()
        eos_date = row.get(eos_key, "").strip()

        if not tag or not eos_date:
            continue
        if tag not in eos_dict:
            eos_dict[tag] = {"eos_date": eos_date}

    return eos_dict

def add_node_names_to_dict(data_dict, inventory_dir):
    """
    Go through YAML files in the inventory directory and add node names to matching service tags.
    """
    for filename in os.listdir(inventory_dir):
        if filename.endswith(".yml") and "talapas.uoregon.edu" in filename:
            node_name = filename.split(".")[0]
            filepath = os.path.join(inventory_dir, filename)

            try:
                with open(filepath, 'r', encoding='utf-8') as file:
                    lines = file.readlines()
                    if len(lines) < 2:
                        continue

                    for line in lines:
                        if "service_tag:" in line:
                            service_tag = line.split(":", 1)[1].strip().upper()
                            if service_tag in data_dict:
                                data_dict[service_tag]["node_name"] = node_name
                            break

            except Exception as e:
                print(f"Error reading {filename}: {e}")

    return data_dict

def add_idrac_ip_to_dict(data_dict):
    """
    Add iDRAC IP addresses to the existing dictionary entries based on their node_name.
    """
    for service_tag, info in data_dict.items():
        node_name = info.get("node_name")
        if not node_name:
            continue  # Skip entries without a node_name

        host_name = f"{node_name}-mgmt.talapas.uoregon.edu"
        try:
            result = subprocess.run(
                f"host {host_name}",
                shell=True,
                capture_output=True,
                text=True
            )
            output_lines = result.stdout.strip().splitlines()
            ip_address = None
            for line in output_lines:
                if "has address" in line:
                    ip_address = line.split("has address")[-1].strip()
                    break  # We only care about the first valid address

            if ip_address:
                data_dict[service_tag]["idrac_ip"] = ip_address
                print(f"Added iDRAC IP for {node_name}: {ip_address}")
            else:
                print(f"No valid IP found for {node_name} in DNS output.")

        except Exception as e:
            print(f"Error during DNS lookup for {host_name}: {e}")

    return data_dict

def save_dict_to_csv(data_dict, output_path):
    """
    Save the EOS dictionary to a CSV file with Service Tag, End of Service Date, Node Name, and iDRAC IP.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)

        # Write fixed headers
        headers = ["Service Tag", "End of Service Date", "Node Name", "iDRAC IP"]
        writer.writerow(headers)

        for service_tag, data in sorted(data_dict.items()):
            row = [
                service_tag,
                data.get("eos_date", ""),
                data.get("node_name", ""),
                data.get("idrac_ip", "")
            ]
            writer.writerow(row)

    print(f"\n Saved {len(data_dict)} entries to '{output_path}'.")

def clean_csv(input_csv, output_csv="output_clean.csv"):
    """
    Reads the input CSV and writes a cleaned CSV with only rows that have a Node Name.
    Returns a cleaned dictionary as well.
    """
    input_csv = Path(input_csv)
    output_csv = Path(output_csv)

    cleaned_dict = {}

    with input_csv.open('r', newline='', encoding='utf-8') as infile, output_csv.open('w', newline='', encoding='utf-8') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)

        writer.writeheader()
        count_kept = 0
        count_removed = 0

        for row in reader:
            node_name = row.get("Node Name", "").strip()
            if node_name:
                # Write the row to the cleaned CSV
                writer.writerow(row)
                count_kept += 1

                # Add to cleaned dictionary
                service_tag = row.get("Service Tag", "").strip()
                cleaned_dict[service_tag] = {
                    "eos_date": row.get("End of Service Date", "").strip(),
                    "node_name": node_name,
                    "idrac_ip": row.get("iDRAC IP", "").strip()
                }
            else:
                count_removed += 1

    print(f"\n Cleaned CSV written to '{output_csv}'")
    print(f"{count_kept} rows kept, {count_removed} rows removed (no Node Name).")

    return cleaned_dict

def log_unedited_files(unedited_logs, output_path="unedited.csv"):
    """
    Write a CSV log of all YAML files that were not edited and the reason why.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Node Name", "Service Tag", "Reason"])

        for log in unedited_logs:
            writer.writerow([log.get("node_name", ""), log.get("service_tag", ""), log.get("reason", "")])

    print(f"\nLogged {len(unedited_logs)} unedited files to '{output_path}'.")


def insert_or_replace_key_in_place(data, key, value, after=None):
    if key in data and str(data[key]) == str(value):
        return False

    if key in data:
        del data[key]

    if after and after in data:
        new_map = CommentedMap()
        for k, v in data.items():
            new_map[k] = v
            if k == after:
                new_map[key] = value
        data.clear()
        data.update(new_map)
    else:
        data[key] = value

    return True

def patch_slurm_partition_indents(filepath):
    """Ensure slurm_partitions list uses 2-space indents."""
    with open(filepath, "r+", encoding="utf-8") as f:
        lines = f.readlines()
        in_part_block = False
        for i in range(len(lines)):
            line = lines[i]
            if line.strip().startswith("slurm_partitions:"):
                in_part_block = True
                continue
            if in_part_block:
                if line.strip().startswith("- "):
                    lines[i] = "  " + line.strip() + "\n"
                elif line.strip() == "" or not line.startswith(" "):
                    in_part_block = False  # end of block
        f.seek(0)
        f.writelines(lines)

def edit_inventory_files(clean_dict):
    inventory_dir = Path(__file__).resolve().parent.parent / "inventory" / "host_vars"
    unedited_logs = []

    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.explicit_start = True
    yaml.indent(mapping=2, sequence=2, offset=0)

    for file in os.listdir(inventory_dir):
        if not file.endswith(".yml") or "talapas.uoregon.edu" not in file:
            continue

        node_name = file.split(".")[0]
        filepath = inventory_dir / file
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = yaml.load(f)

            if not isinstance(data, CommentedMap):
                unedited_logs.append({
                    "node_name": node_name,
                    "service_tag": "",
                    "reason": "Invalid YAML format"
                })
                continue

            service_tag = str(data.get("dell_service_tag", "")).strip().upper()
            if not service_tag:
                unedited_logs.append({
                    "node_name": node_name,
                    "service_tag": "",
                    "reason": "Missing dell_service_tag"
                })
                continue

            info = clean_dict.get(service_tag)
            if not info:
                unedited_logs.append({
                    "node_name": node_name,
                    "service_tag": service_tag,
                    "reason": "Service tag not in clean_dict"
                })
                continue

            eos = info.get("eos_date", "").strip()
            idrac = info.get("idrac_ip", "").strip()
            modified = False

            if eos:
                modified |= insert_or_replace_key_in_place(data, "dell_eos", eos, after="dell_service_tag")

            if idrac:
                if "public_mac" in data:
                    modified |= insert_or_replace_key_in_place(data, "idrac_ip", idrac, after="public_mac")
                elif "public_ip" in data:
                    modified |= insert_or_replace_key_in_place(data, "idrac_ip", idrac, after="public_ip")
                else:
                    modified |= insert_or_replace_key_in_place(data, "idrac_ip", idrac)

            if modified:
                with open(filepath, "w", encoding="utf-8") as f:
                    yaml.dump(data, f)
                patch_slurm_partition_indents(filepath)
                print(f"Updated: {file}")
            else:
                unedited_logs.append({
                    "node_name": node_name,
                    "service_tag": service_tag,
                    "reason": "No changes needed"
                })

        except Exception as e:
            unedited_logs.append({
                "node_name": node_name,
                "service_tag": "",
                "reason": f"Error: {str(e)}"
            })
        
    log_unedited_files(unedited_logs)


if __name__ == "__main__":

    eos_data = {}

    filepaths = [
        "../racs-internal-docs/notes/flopez2/dell_eos_documents/IBExport_03-29-2023_09_50_54_PM(IBExport_03-29-2023_09_50_54_PM).csv",
        "../racs-internal-docs/notes/flopez2/dell_eos_documents/IBExport_04-25-2025_03_15_42_PM(IBExport_04-25-2025_03_15_42_PM).csv",
        "../racs-internal-docs/notes/flopez2/dell_eos_documents/report1691613781696(report1691613781696).csv",
        "../racs-internal-docs/notes/flopez2/dell_eos_documents/report1710953293279(report1710953293279).csv"
    ]

    for filepath in filepaths:
        try:
            eos_data = update_eos_dictionary(filepath, eos_data)
        except Exception as e:
            print(f"Error processing {filepath}: {e}")

    parent_dir = Path(__file__).resolve().parent.parent
    inventory_path = parent_dir / "inventory" / "host_vars"

    add_node_names_to_dict(eos_data, inventory_path)

    full_dict = add_idrac_ip_to_dict(eos_data)

    save_dict_to_csv(full_dict, "output.csv")
    clean_dict = clean_csv("output.csv", "output_clean.csv")
    edit_inventory_files(clean_dict)