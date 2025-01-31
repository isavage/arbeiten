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
        # Source profile to get OGG_HOME
        source ~/.bash_profile
        
        # Verify OGG_HOME exists
        if [ -z \"\$OGG_HOME\" ]; then
            echo \"Error: OGG_HOME not set on ${server}\"
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
        
        echo \"\$status_output\"
    " | while IFS= read -r line; do
        if [[ $line =~ ^Error: ]]; then
            # Write error message to HTML
            echo "<tr class=\"status-red\">" >> ${HTML_REPORT}
            echo "<td>${server}</td>" >> ${HTML_REPORT}
            echo "<td colspan=\"5\">$line</td>" >> ${HTML_REPORT}
            echo "</tr>" >> ${HTML_REPORT}
            continue
        fi
        
        if [[ $line =~ EXTRACT|REPLICAT ]]; then
            process_name=$(echo $line | awk '{print $2}')
            status=$(echo $line | awk '{print $3}')
            lag=$(echo $line | grep -oP 'LAG.*?,' | cut -d' ' -f2)
            checkpoint_lag=$(echo $line | grep -oP 'CHKPT.*?,' | cut -d' ' -f2)
            
            # Calculate RBA movement
            rba_status="Moving"
            if [[ ! -z "$prev_rba" && "$current_rba" == "$prev_rba" ]]; then
                rba_status="Not Moving"
            fi
            
            # Determine status color
            status_class="status-green"
            if [[ "$status" == "ABENDED" || "$status" == "STOPPED" ]]; then
                status_class="status-red"
            elif [[ "$lag" =~ ^[0-9]+$ && "$lag" -gt 600 || "$rba_status" == "Not Moving" ]]; then
                status_class="status-orange"
            fi
            
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
