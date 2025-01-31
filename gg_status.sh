#!/bin/bash

MAILTO=

# Two separate server lists
LIST1=(
    "server1"
    "server2"
    "server3"
)

LIST2=(
    "server4"
    "server5"
    "server6"
)

# File to store previous RBA values
RBA_FILE="/tmp/gg_rba_values.txt"

# HTML report location
HTML_REPORT="/var/www/html/gg_status.html"

# Function to generate HTML header
generate_html_header() {
    cat > ${HTML_REPORT} << EOF
<!DOCTYPE html>
<html>
<head>
    <title>GoldenGate Status Report</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            font-size: 12px;
        }
        .outer-table {
            width: 100%;
            border-collapse: collapse;
        }
        .outer-td {
            width: 50%;
            vertical-align: top;
        }
        .server-table {
            border-collapse: collapse;
            width: auto;
        }
        .server-table th, .server-table td {
            border: 1px solid #ddd;
            text-align: left;
            line-height: 14px;
            height: 14px;
            vertical-align: middle;
            white-space: nowrap;
        }
        .server-table th {
            background-color: #000000;
            color: white;
            font-weight: normal;
            height: 16px;
            line-height: 16px;
            padding: 0 4px;
        }
        .server-header td {
            background-color: white;
            color: black;
            font-weight: bold;
            text-align: center !important;
            height: 18px;
            line-height: 18px;
            border-bottom: 2px solid #ddd;
            padding: 0 4px;
        }
        .server-table td {
            padding: 0 4px;
        }
        .col-process { min-width: 60px; }
        .col-status { min-width: 70px; }
        .col-lag { min-width: 65px; }
        .col-chkpt { min-width: 65px; }
        .col-rba { min-width: 60px; }
        .status-red { color: #ff0000; font-weight: bold; }
        .status-orange { color: #ff8c00; font-weight: bold; }
        .status-green { color: #008000; font-weight: bold; }
        .header {
            text-align: center;
        }
        .header h2 { margin: 0; }
        .header p { margin: 0; }
    </style>
</head>
<body>
    <div class="header">
        <h2>GoldenGate Status Report</h2>
        <p>Last Updated: $(date '+%Y-%m-%d %H:%M:%S')</p>
    </div>
    <table class="outer-table">
EOF
}

# Function to start a new row in the outer table
start_outer_row() {
    echo "<tr>" >> ${HTML_REPORT}
}

# Function to end a row in the outer table
end_outer_row() {
    echo "</tr>" >> ${HTML_REPORT}
}

# Function to generate server table header
generate_server_table_header() {
    local server=$1
    cat >> ${HTML_REPORT} << EOF
<td class="outer-td">
<table class="server-table">
    <tr class="server-header"><td colspan="5">$server</td></tr>
    <tr>
        <th class="col-process">Process</th>
        <th class="col-status">Status</th>
        <th class="col-lag">Lag</th>
        <th class="col-chkpt">Chkpt Lag</th>
        <th class="col-rba">RBA</th>
    </tr>
EOF
}

# Function to end server table
end_server_table() {
    echo "</table></td>" >> ${HTML_REPORT}
}

# Function to extract lag value
get_lag_value() {
    local line="$1"
    echo "$line" | awk '{
        if (NF >= 4) {
            print $4
        }
    }'
}

# Function to extract checkpoint lag
get_checkpoint_lag() {
    local line="$1"
    echo "$line" | awk '{
        if (NF >= 5) {
            print $5
        }
    }'
}

# Function to get RBA/SCN for a process
get_process_position() {
    local process_name="$1"
    local process_type="$2"
    local position_output="$3"
    local position=""
    
    if [[ "$process_type" == "EXTRACT" ]]; then
        # First try to get SCN format (for regular extracts)
        position=$(echo "$position_output" | grep "SCN" | grep -o "([0-9]*)" | tr -d '()')
        
        # If SCN not found, try RBA format (for pumps)
        if [[ -z "$position" ]]; then
            position=$(echo "$position_output" | grep "RBA" | awk '{print $NF}')
        fi
    else
        # For replicat, get number after "RBA"
        position=$(echo "$position_output" | grep "RBA" | awk '{print $NF}')
    fi
    
    echo "$position"
}

# Function to check GoldenGate status and generate HTML
check_gg_status() {
    local server=$1
    declare -A current_positions
    declare -A prev_positions
    declare -A process_types
    
    # Get previous positions
    if [ -f ${RBA_FILE} ]; then
        while IFS='|' read -r timestamp srv data; do
            if [ "$srv" = "$server" ]; then
                # Parse space-separated process:position pairs
                for pair in $data; do
                    proc=$(echo "$pair" | cut -d: -f1)
                    pos=$(echo "$pair" | cut -d: -f2)
                    if [ ! -z "$proc" ] && [ ! -z "$pos" ]; then
                        prev_positions[$proc]=$pos
                    fi
                done
            fi
        done < ${RBA_FILE}
    fi

    ssh oracle@${server} "
        # Source profile to get OGG_HOME
        if [ -f ~/.bash_profile ]; then
            . ~/.bash_profile
        elif [ -f ~/.profile ]; then
            . ~/.profile
        elif [ -f ~/.bashrc ]; then
            . ~/.bashrc
        fi
        
        # Verify OGG_HOME exists and is valid
        if [ -z \"\$OGG_HOME\" ] || [ ! -f \"\$OGG_HOME/ggsci\" ]; then
            echo \"Error: Cannot find valid OGG_HOME on ${server}\"
            exit 1
        fi
        
        cd \$OGG_HOME || exit 1
        
        # Get list of processes first
        process_list=\$(./ggsci << EOF
        info all
EOF
        )
        
        echo \"---BEGIN_PROCESS_LIST---\"
        echo \"\$process_list\"
        echo \"---BEGIN_POSITIONS---\"
        
        # Get position for each process
        while read -r line; do
            if echo \"\$line\" | grep -q \"^EXTRACT\\|^REPLICAT\"; then
                process_type=\$(echo \"\$line\" | awk '{print \$1}')
                process_name=\$(echo \"\$line\" | awk '{print \$3}')
                
                position_output=\$(./ggsci << EOF
                info \${process_type} \${process_name}
EOF
                )
                echo \"---PROCESS_\${process_name}---\"
                echo \"\$position_output\"
            fi
        done <<< \"\$process_list\"
    " | while IFS= read -r line; do
        if [ "$line" = "---BEGIN_PROCESS_LIST---" ]; then
            reading_processes=1
            reading_positions=0
            continue
        elif [ "$line" = "---BEGIN_POSITIONS---" ]; then
            reading_processes=0
            reading_positions=1
            current_process=""
            position_output=""
            continue
        elif echo "$line" | grep -q "^---PROCESS_.*---"; then
            # Store position for previous process if exists
            if [ ! -z "$current_process" ] && [ ! -z "$position_output" ]; then
                current_positions[$current_process]=$(get_process_position "$current_process" "${process_types[$current_process]}" "$position_output")
            fi
            current_process=$(echo "$line" | sed 's/^---PROCESS_\(.*\)---$/\1/')
            position_output=""
            continue
        fi

        # Collect process information
        if [ $reading_processes -eq 1 ] && echo "$line" | grep -q "^EXTRACT\|^REPLICAT"; then
            process_type=$(echo "$line" | awk '{print $1}')
            status=$(echo "$line" | awk '{print $2}')
            process_name=$(echo "$line" | awk '{print $3}')
            lag=$(get_lag_value "$line")
            checkpoint_lag=$(get_checkpoint_lag "$line")
            
            # Store process type for position extraction
            process_types[$process_name]=$process_type
            
            # Convert lag to minutes for comparison (format is HH:MM:SS)
            lag_minutes=0
            if echo "$lag" | grep -q "^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]$"; then
                hours=$(echo "$lag" | cut -d: -f1)
                minutes=$(echo "$lag" | cut -d: -f2)
                seconds=$(echo "$lag" | cut -d: -f3)
                lag_minutes=$((10#$hours * 60 + 10#$minutes + (10#$seconds >= 30 ? 1 : 0)))
            fi
            
            # Determine status color
            status_class="status-green"
            if [ "$status" != "RUNNING" ]; then
                status_class="status-red"
            elif [ $lag_minutes -gt 10 ]; then
                status_class="status-orange"
            fi
            
            # Set default values without quotes
            : ${lag:=00:00:00}
            : ${checkpoint_lag:=00:00:00}
            
            # Write to HTML with column classes
            echo "<tr>" >> ${HTML_REPORT}
            echo "<td class=\"col-process\">${process_name}</td>" >> ${HTML_REPORT}
            echo "<td class=\"col-status ${status_class}\">${status}</td>" >> ${HTML_REPORT}
            echo "<td class=\"col-lag\">${lag}</td>" >> ${HTML_REPORT}
            echo "<td class=\"col-chkpt\">${checkpoint_lag}</td>" >> ${HTML_REPORT}
            echo "<td class=\"col-rba ${status_class}\">Moving</td>" >> ${HTML_REPORT}
            echo "</tr>" >> ${HTML_REPORT}
        fi
        
        # Collect position output
        if [ $reading_positions -eq 1 ] && [ ! -z "$current_process" ]; then
            position_output+="$line"$'\n'
        fi
    done
    
    # Store current positions for next run
    positions_data=""
    for proc in "${!current_positions[@]}"; do
        positions_data+="${proc}:${current_positions[$proc]} "
    done
    echo "$(date '+%Y-%m-%d %H:%M:%S')|${server}|${positions_data}" >> ${RBA_FILE}
}

# Main execution
generate_html_header

# Calculate the maximum number of rows needed
max_rows=$(( ${#LIST1[@]} > ${#LIST2[@]} ? ${#LIST1[@]} : ${#LIST2[@]} ))

# Process servers in pairs
for ((i=0; i<max_rows; i++)); do
    start_outer_row
    
    # Left column server (LIST1)
    if ((i < ${#LIST1[@]})); then
        generate_server_table_header "${LIST1[i]}"
        check_gg_status "${LIST1[i]}"
        end_server_table
    else
        # Empty cell if no more left servers
        echo "<td class=\"outer-td\"></td>" >> ${HTML_REPORT}
    fi
    
    # Right column server (LIST2)
    if ((i < ${#LIST2[@]})); then
        generate_server_table_header "${LIST2[i]}"
        check_gg_status "${LIST2[i]}"
        end_server_table
    else
        # Empty cell if no more right servers
        echo "<td class=\"outer-td\"></td>" >> ${HTML_REPORT}
    fi
    
    end_outer_row
done

# Generate HTML footer
cat >> ${HTML_REPORT} << EOF
    </table>
</body>
</html>
EOF

# Cleanup old RBA values (keep last 48 hours)
find ${RBA_FILE} -mtime +2 -delete 2>/dev/null

# Set appropriate permissions for web server
chmod 644 ${HTML_REPORT}

(
echo "To: "${MAILTO}
echo "Subject: PT GG: Status"
echo "Content-Type: text/html"
cat ${HTML_REPORT}
echo
) | /usr/sbin/sendmail -t
