#!/bin/bash

MAILTO=

# Two separate server lists
LIST1=(
    "server1"
    "server2"
)

LIST2=(
    "server1"
    "server2"
)

# File to store previous RBA values
RBA_FILE="gg_rba_values.txt"

# HTML report location
HTML_REPORT="gg_status.html"


# Function to generate HTML header
generate_html_header() {
    cat > ${HTML_REPORT} << EOF
<!DOCTYPE html>
<html>
<head>
    <title>GoldenGate Status Report</title>
    <style>
        body {
            margin: 0;
            padding: 10px;
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
            padding: 5px;
        }
        .server-table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 15px;
        }
        .server-table th, .server-table td {
            border: 1px solid #ddd;
            padding: 2px 4px;
            text-align: left;
            height: 16px;
            white-space: nowrap;
        }
        .server-table th {
            background-color: #000000;
            color: white;
            height: 18px;
        }
        .server-header {
            background-color: #4a4a4a;
            color: white;
            font-weight: bold;
            padding: 2px 4px;
        }
        .status-red {
            color: #ff0000;
            font-weight: bold;
        }
        .status-orange {
            color: #ff8c00;
            font-weight: bold;
        }
        .status-green {
            color: #008000;
            font-weight: bold;
        }
        .header {
            text-align: center;
            padding: 5px;
            margin-bottom: 10px;
        }
        .header h2 {
            margin: 0 0 5px 0;
        }
        .header p {
            margin: 0;
        }
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
    <tr><td colspan="5" class="server-header">$server</td></tr>
    <tr>
        <th>Process</th>
        <th>Status</th>
        <th>Lag</th>
        <th>Chkpt Lag</th>
        <th>RBA</th>
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
        if (NF >= 5) {
            print $4
        }
    }'
}

# Function to extract checkpoint lag
get_checkpoint_lag() {
    local line="$1"
    echo "$line" | awk '{
        if (NF >= 6) {
            print $5
        }
    }'
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
  ssh ${server} "
            . ~/.profile

# Verify OGG_HOME exists and is valid
        if [ -z \"\$OGG_HOME\" ] || [ ! -f \"\$OGG_HOME/ggsci\" ]; then
            echo \"Error: Cannot find valid OGG_HOME on ${server}\"
            exit 1
        fi

        cd \$OGG_HOME || exit 1

        # Get current timestamp
        timestamp=\$(date '+%Y-%m-%d %H:%M:%S')

        # Get status
        status_output=\$(./ggsci << EOF
        info all
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
            echo "<tr>" >> ${HTML_REPORT}
            echo "<td colspan=\"5\" class=\"status-red\">$line</td>" >> ${HTML_REPORT}
            echo "</tr>" >> ${HTML_REPORT}
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
            if [[ $lag =~ ([0-9]{2}):([0-9]{2}):([0-9]{2}) ]]; then
                hours=$((10#${BASH_REMATCH[1]}))
                minutes=$((10#${BASH_REMATCH[2]}))
                seconds=$((10#${BASH_REMATCH[3]}))
                lag_minutes=$((hours * 60 + minutes + (seconds >= 30 ? 1 : 0)))
            fi

            # Calculate RBA movement
            rba_status="Moving"
            if [[ ! -z "$prev_rba" && "$current_rba" == "$prev_rba" ]]; then
                rba_status="Not Moving"
                status_class="status-orange"
            fi
            
            # Determine status color
            status_class="status-green"
            if [[ "$status" != "RUNNING" ]]; then
                status_class="status-red"
            elif [[ $lag_minutes -gt 10 || "$rba_status" == "Not Moving" ]]; then
                status_class="status-orange"
            fi


            # Set default values without quotes
            : ${lag:=00:00:00}
            : ${checkpoint_lag:=00:00:00}

            # Write to HTML
            echo "<tr>" >> ${HTML_REPORT}
            echo "<td>${process_name}</td>" >> ${HTML_REPORT}
            echo "<td class=\"${status_class}\">${status}</td>" >> ${HTML_REPORT}
            echo "<td>${lag}</td>" >> ${HTML_REPORT}
            echo "<td>${checkpoint_lag}</td>" >> ${HTML_REPORT}
            echo "<td class=\"${status_class}\">${rba_status}</td>" >> ${HTML_REPORT}
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
