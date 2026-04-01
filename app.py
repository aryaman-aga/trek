import streamlit as st
import subprocess
import os
import sys
import re
import time
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
if st.button("🚀 Run Scraper & Download"):
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
