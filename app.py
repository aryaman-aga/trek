import streamlit as st
import subprocess
import os
import sys
import re
import time
import requests
import zipfile
import io
from datetime import datetime
import uuid

# Set page config
st.set_page_config(page_title="Cycle Brand Scraper", layout="centered")

st.title("🚲 Cycle Brand Scraper")
st.write("Welcome! Use this tool to select cycle brands and automatically generate an Excel report. Please select the script you want to run and the brands you want to scrape.")

# Define the two available scripts and their output files
SCRIPTS = {
    "Global Brands": {
        "file": "scrape_competition_matrix.py",
        "output": "competition_bike_models.xlsx"
    },
    "All Indian Cycle Brands": {
        "file": "scrape_all_brands.py",
        "output": "all_bike_models.xlsx"
    }
}

# --- 1. Select Script ---
selected_script_label = st.radio("Select which scraper to run:", list(SCRIPTS.keys()))
script_info = SCRIPTS[selected_script_label]
script_file = script_info["file"]
output_file = script_info["output"]

# --- 2. Parse Available Brands from the chosen script ---
@st.cache_data
def get_brands(filename):
    brands = []
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            content = f.read()
            # Find all patterns like "name": "BrandName"
            matches = re.findall(r'"name":\s*"([^"]+)"', content)
            seen = set()
            for m in matches:
                # Basic cleanup/filter if any variable names leaked through
                if m not in seen and len(m) > 1:
                    seen.add(m)
                    brands.append(m)
    return sorted(brands)

brands_list = get_brands(script_file)

# --- 3. Select Brands ---
st.subheader("Settings & Brands")
if not brands_list:
    st.error(f"Could not find any brands inside `{script_file}`.")
    st.stop()

# Utility to toggle all checks
select_all = st.checkbox("Select All Brands", value=False)
if select_all:
    selected_brands = st.multiselect("Choose brands to scrape:", brands_list, default=brands_list)
else:
    selected_brands = st.multiselect("Choose brands to scrape:", brands_list)

convert_to_inr = st.checkbox("🔄 Convert foreign rates to Indian Rupees (INR)", value=True, help="If checked, foreign prices will be converted to INR. Otherwise, they will remain in their original currencies if available.")

# --- 4. Execution ---
st.write("---")

col1, col2 = st.columns(2)

with col1:
    run_local = st.button("🚀 Run Local (Slower)", help="Runs on Render. Could crash if multiple users click at once.")

with col2:
    run_cloud = st.button("☁️ Queue API Cloud Job (Fast)", help="Uses GitHub Actions to scrape. Will be available under GitHub Actions tab in 2 minutes.")

if run_cloud:
    if not selected_brands:
        st.warning("Please select at least one brand before running.")
    else:
        # GitHub trigger
        github_token = os.environ.get("GITHUB_PAT")
        if not github_token:
            st.error("Missing GITHUB_PAT. Add a GitHub Personal Access Token to Render environment variables to use this feature.")
        else:
            import requests

            status_text = st.empty()
            status_text.info("Queuing job in GitHub Actions...")
            repo = "aryaman-aga/trek"
            workflow_id = "scrape_worker.yml"
            url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_id}/dispatches"
            heads = {
                "Authorization": f"Bearer {github_token}",
                "Accept": "application/vnd.github.v3+json"
            }
            payload = {
                "ref": "main",
                "inputs": {
                    "script": script_file,
                    "brands": " ".join([b.lower() for b in selected_brands]),
                    "convert_to_inr": "true" if convert_to_inr else "false"
                }
            }
            resp = requests.post(url, headers=heads, json=payload)
            if resp.status_code == 204:
                status_text.success(f"✅ Job sent successfully! The data will be ready under 'Actions' -> 'Background Scraper Worker' in your repo in a few minutes.")
                st.markdown(f"[View Job Status Here](https://github.com/{repo}/actions)")
            else:
                status_text.error(f"Failed to queue job: {resp.text}")

if run_local:
    if not selected_brands:
        st.warning("Please select at least one brand before running.")
    else:
        # Show a progress bar and status text
        progress_bar = st.progress(0)
        status_text = st.empty()
        log_expander = st.expander("Live Logs", expanded=False)
        log_container = log_expander.empty()
        
        status_text.info(f"Starting scrape for {len(selected_brands)} brand(s)...")

        # We pass the selected brand names in lowercase as command-line arguments to the script
        # Add "-u" to force unbuffered output so we get python prints immediately
        args = [sys.executable, "-u", script_file] + [b.lower() for b in selected_brands]
        if convert_to_inr:
            args.append("--inr")
        
        # Generate unique output file to avoid concurrency issues and add timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        unique_id = uuid.uuid4().hex[:6]
        base_name = output_file.replace(".xlsx", "")
        unique_output_file = f"{base_name}_{timestamp}_{unique_id}.xlsx"
        
        args.extend(["--out", unique_output_file])
            
        try:
            start_time = time.time()
            
            # Run the subprocess and stream the output
            process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            
            completed_count = 0
            total_count = len(selected_brands)
            logs = []
            
            for line in process.stdout:
                logs.append(line)
                # Check for completion markers from either script
                if "[DONE]" in line or "rows collected" in line:
                    completed_count += 1
                    pct = min(completed_count / total_count, 1.0)
                    progress_bar.progress(pct)
                    
                    # Calculate ETA
                    elapsed_time = time.time() - start_time
                    time_per_brand = elapsed_time / completed_count
                    brands_left = total_count - completed_count
                    eta_seconds = int(brands_left * time_per_brand)
                    
                    if eta_seconds > 60:
                        eta_str = f"{eta_seconds // 60}m {eta_seconds % 60}s"
                    else:
                        eta_str = f"{eta_seconds}s"
                    
                    if completed_count < total_count:
                        status_text.info(f"Processing... {completed_count}/{total_count} brands completed. ETA: ~{eta_str} left")
                    else:
                        status_text.info(f"Processing... {completed_count}/{total_count} brands completed.")
                    
                # Update logs periodically to avoid slowing down UI
                if len(logs) % 5 == 0:
                    log_container.code("".join(logs[-100:])) # show last 100 lines
            
            process.wait()
            log_container.code("".join(logs)) # Final log update
            
            # Check if the Excel output file was successfully created
            if process.returncode == 0 and os.path.exists(unique_output_file):
                progress_bar.progress(1.0)
                status_text.success("✅ Scraping completed successfully!")
                
                # Provide download button
                with open(unique_output_file, "rb") as file_data:
                    st.download_button(
                        label="📥 Download Excel Report",
                        data=file_data,
                        file_name=unique_output_file,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            else:
                status_text.error("❌ Scraping finished but no Excel file was created or an error occurred.")
                st.write("Please check the Live Logs above for details.")
        except Exception as e:
            status_text.error(f"Failed to execute the script: {str(e)}")

# --- 5. Download Cloud Scrapes ---
st.write("---")
st.subheader("📥 Recent Cloud Scrapes")
st.markdown("If you queued a cloud job, you can download the completed Excel reports below.")

github_token = os.environ.get("GITHUB_PAT")
if not github_token:
    st.info("Please set the `GITHUB_PAT` environment variable in Render to view cloud scrape results.")
else:
    repo = "aryaman-aga/trek"
    url = f"https://api.github.com/repos/{repo}/actions/artifacts"
    heads = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }

    colA, colB = st.columns([1, 1])
    with colA:
        if st.button("🔄 Refresh Cloud Data"):
            pass
    with colB:
        if st.button("🗑️ Delete All"):
            with st.spinner("Deleting all artifacts on GitHub..."):
                del_resp = requests.get(url, headers=heads)
                if del_resp.status_code == 200:
                    artifacts_to_del = del_resp.json().get("artifacts", [])
                    for a in artifacts_to_del:
                        requests.delete(f"https://api.github.com/repos/{repo}/actions/artifacts/{a['id']}", headers=heads)
                    st.success("Deleted all scrapes!")
                    time.sleep(1)
                    st.rerun()

    try:
        resp = requests.get(url, headers=heads)
        if resp.status_code == 200:
            artifacts = resp.json().get("artifacts", [])
            if not artifacts:
                st.write("No scripts have completed yet.")
            else:
                for art in artifacts[:5]:  # Show local recent artifacts
                    col1, col2, col3 = st.columns([4, 4, 3])
                    
                    with col1:
                        st.write(f"📁 **{art['name']}**")
                    with col2:
                        try:
                            # Format nicely
                            dt = datetime.strptime(art['created_at'], "%Y-%m-%dT%H:%M:%SZ")
                            st.write(f"⏰ {dt.strftime('%b %d, %H:%M UTC')}")
                        except:
                            st.write(art['created_at'])
                            
                    with col3:
                        if st.button("Fetch", key=f"fetch_{art['id']}"):
                            with st.spinner("Downloading artifact..."):
                                zip_resp = requests.get(art['archive_download_url'], headers=heads)
                                if zip_resp.status_code == 200:
                                    # Unzip in memory to extract the xlsx file
                                    with zipfile.ZipFile(io.BytesIO(zip_resp.content)) as z:
                                        xlsx_files = [f for f in z.namelist() if f.endswith(".xlsx")]
                                        if xlsx_files:
                                            target_file = xlsx_files[0]
                                            file_data = z.read(target_file)
                                            st.download_button(
                                                label="📥 Download Excel",
                                                data=file_data,
                                                file_name=target_file,
                                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                                key=f"dl_{art['id']}"
                                            )
                                        else:
                                            st.warning("No Excel file found inside this artifact.")
                                else:
                                    st.error("Failed to download zip from GitHub.")
        elif resp.status_code in (401, 403):
             st.error("GitHub access denied. Make sure your GITHUB_PAT is correct and has the `workflow` scope.")
        else:
             st.error(f"Failed to fetch list. Status: {resp.status_code}")
             
    except Exception as e:
        st.error(f"An error occurred fetching results: {str(e)}")
