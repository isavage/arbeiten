#!/bin/bash

# List of servers to monitor
SERVERS=(
    "server1"
    "server2"
    "server3"
)

# File to store previous RBA values
RBA_FILE="/tmp/gg_rba_values.txt"

# HTML report location
HTML_REPORT="/var/www/html/gg_status.html"

# Function to extract lag value (Lag at Chkpt)
get_lag_value() {
    local line="$1"
    echo "$line" | awk '{
        if (NF >= 5) {
            print $4
        }
    }'
}

# Function to extract time since checkpoint
get_checkpoint_lag() {
    local line="$1"
    echo "$line" | awk '{
        if (NF >= 6) {
            print $5
        }
    }'
}

# Function to generate HTML header
generate_html_header() {
    cat > ${HTML_REPORT} << EOF
<!DOCTYPE html>
<html>
<head>
    <title>GoldenGate Status Report</title>
    <style>
        table {
            border-collapse: collapse;
            width: 100%;
            margin: 20px 0;
        }
        th, td {
            border: 1px solid #ddd;
            padding: 8px;
            text-align: left;
        }
        th {
            background-color: #4CAF50;
            color: white;
        }
        tr:nth-child(even) {
            background-color: #f2f2f2;
        }
        .status-red {
            background-color: #ff6666;
        }
        .status-orange {
            background-color: #ffb366;
        }
        .status-green {
            background-color: #90EE90;
        }
        .header {
            text-align: center;
            padding: 20px;
        }
    </style>
</head>
<body>
    <div class="header">
        <h2>GoldenGate Status Report</h2>
        <p>Last Updated: $(date '+%Y-%m-%d %H:%M:%S')</p>
    </div>
    <table>
        <tr>
            <th>Server</th>
            <th>Process Name</th>
            <th>Status</th>
            <th>Lag</th>
            <th>Checkpoint Lag</th>
            <th>RBA Movement</th>
        </tr>
EOF
}

# Function to check GoldenGate status and generate HTML
check_gg_status() {
    local server=$1
    
    # Get previous RBA values
    local prev_rba=""
    if [ -f ${RBA_FILE} ]; then
        prev_rba=$(grep "|${server}|" ${RBA_FILE} | tail -n 1)
    fi

    # SSH to server and execute commands
   ssh oracle@${server} "
        # Try to source profile files in order of preference
        if [ -f ~/.bash_profile ]; then
            . ~/.bash_profile
        elif [ -f ~/.profile ]; then
            . ~/.profile
        elif [ -f ~/.bashrc ]; then
            . ~/.bashrc
        fi
        
        # If OGG_HOME is not set, try to find it
        if [ -z \"\$OGG_HOME\" ]; then
            # Common GoldenGate installation paths
            possible_paths=(
                \"/oracle/app/goldengate\"
                \"/oracle/goldengate\"
                \"/u01/app/goldengate\"
                \"/home/oracle/goldengate\"
            )
            
            for path in \"\${possible_paths[@]}\"; do
                if [ -d \"\$path\" ] && [ -f \"\$path/ggsci\" ]; then
                    OGG_HOME=\"\$path\"
                    break
                fi
            done
        fi
        
        # Verify OGG_HOME exists and is valid
        if [ -z \"\$OGG_HOME\" ] || [ ! -f \"\$OGG_HOME/ggsci\" ]; then
            echo \"Error: Cannot find valid OGG_HOME on ${server}\"
            exit 1
        fi
        
        cd \$OGG_HOME || exit 1
        
        # Get current timestamp
        timestamp=\$(date '+%Y-%m-%d %H:%M:%S')
        
        # Get detailed status
        status_output=\$(./ggsci << EOF
        info all detail
EOF
        )
        
        # Get current RBA values
        current_rba=\$(./ggsci << EOF
        info extract * showch
EOF
        )
        
        echo \"\$timestamp|\$server|\$current_rba\" >> ${RBA_FILE}
        
        echo \"Using OGG_HOME: \$OGG_HOME\"
        echo \"\$status_output\"
     " | while IFS= read -r line; do
        if [[ $line =~ ^Error: ]]; then
            echo "<tr class=\"status-red\">" >> ${HTML_REPORT}
            echo "<td>${server}</td>" >> ${HTML_REPORT}
            echo "<td colspan=\"5\">$line</td>" >> ${HTML_REPORT}
            echo "</tr>" >> ${HTML_REPORT}
            continue
        fi
        
        if [[ $line =~ ^Using ]]; then
            echo "$line" >&2
            continue
        fi
        
        # Process only EXTRACT and REPLICAT lines
        if [[ $line =~ ^(EXTRACT|REPLICAT) ]]; then
            process_type=$(echo "$line" | awk '{print $1}')
            status=$(echo "$line" | awk '{print $2}')
            process_name=$(echo "$line" | awk '{print $3}')
            lag=$(get_lag_value "$line")
            checkpoint_lag=$(get_checkpoint_lag "$line")
            
            # Convert lag to minutes for comparison (format is HH:MM:SS)
            lag_minutes=0
            if [[ $lag =~ ([0-9]+):([0-9]+):([0-9]+) ]]; then
                hours=${BASH_REMATCH[1]}
                minutes=${BASH_REMATCH[2]}
                seconds=${BASH_REMATCH[3]}
                lag_minutes=$((hours * 60 + minutes + (seconds >= 30 ? 1 : 0)))
            fi
            
            # Determine status color
            status_class="status-green"
            if [[ "$status" != "RUNNING" ]]; then
                status_class="status-red"
            elif [[ $lag_minutes -gt 10 || "$rba_status" == "Not Moving" ]]; then
                status_class="status-orange"
            fi
            
            # Calculate RBA movement
            rba_status="Moving"
            if [[ ! -z "$prev_rba" && "$current_rba" == "$prev_rba" ]]; then
                rba_status="Not Moving"
                status_class="status-orange"
            fi
            
            # Set default values without quotes
            : ${lag:=00:00:00}
            : ${checkpoint_lag:=00:00:00}
            
            # Write to HTML
            echo "<tr class=\"${status_class}\">" >> ${HTML_REPORT}
            echo "<td>${server}</td>" >> ${HTML_REPORT}
            echo "<td>${process_name}</td>" >> ${HTML_REPORT}
            echo "<td>${status}</td>" >> ${HTML_REPORT}
            echo "<td>${lag}</td>" >> ${HTML_REPORT}
            echo "<td>${checkpoint_lag}</td>" >> ${HTML_REPORT}
            echo "<td>${rba_status}</td>" >> ${HTML_REPORT}
            echo "</tr>" >> ${HTML_REPORT}
        fi
    done
}

# Generate HTML footer
generate_html_footer() {
    cat >> ${HTML_REPORT} << EOF
    </table>
</body>
</html>
EOF
}

# Main execution
generate_html_header

# Check each server
for server in "${SERVERS[@]}"; do
    check_gg_status "$server"
done

generate_html_footer

# Cleanup old RBA values (keep last 48 hours)
find ${RBA_FILE} -mtime +2 -delete 2>/dev/null

# Set appropriate permissions for web server
chmod 644 ${HTML_REPORT}
