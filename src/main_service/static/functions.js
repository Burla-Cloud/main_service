let clusterIsOn = false; // Track cluster status globally
let eventSource; // Reuse the EventSource globally

// Function to monitor the cluster status and handle the display of nodes
function monitorCluster() {
    const monitorElement = document.getElementById('monitor-message');
    const clusterStatus = document.getElementById('cluster-status');
    const buttonElement = document.querySelector('button'); // Select the button to change text
    let eventSource = new EventSource('/monitor');  // SSE to stream node statuses
    let nodes = {};  // Object to track the current status of each node

    // Initially set the cluster status to OFF
    clusterStatus.textContent = "OFF";
    updateButtonText("Start Cluster"); // Set button text initially

    eventSource.onmessage = function(event) {
        const data = event.data;
        console.log('Data received:', data);

        // Extract node ID and status from the streamed data
        const nodeIdMatch = data.match(/node_id: (\S+)/);
        const statusMatch = data.match(/==> status: (\S+)/);
        const removedMatch = data.match(/removed from stream/);

        if (nodeIdMatch) {
            const nodeId = nodeIdMatch[1];
            if (statusMatch) {
                const status = statusMatch[1];

                // Only show nodes with status RUNNING or BOOTING
                if (status === "RUNNING" || status === "BOOTING") {
                    nodes[nodeId] = status;  // Update the node's status
                }

                // Update the monitor display and the cluster status based on all nodes
                updateMonitorDisplay(nodes);
                updateClusterStatus(nodes, buttonElement); // Pass buttonElement to update function
            }
        }

        // Handle nodes that are removed from the stream
        if (removedMatch && nodeIdMatch) {
            const nodeId = nodeIdMatch[1];
            delete nodes[nodeId];  // Remove the node from the nodes object
            updateMonitorDisplay(nodes);  // Update the monitor display
            updateClusterStatus(nodes, buttonElement);  // Update the cluster status
        }
    };

    eventSource.onerror = function(error) {
        console.error('Error with EventSource:', error);
        monitorElement.textContent = "Error: Unable to receive updates.";
        eventSource.close();
    };

    // Function to update the monitor display with current node statuses
    function updateMonitorDisplay(nodes) {
        monitorElement.innerHTML = "";  // Clear the current content

        for (const nodeId in nodes) {
            const status = nodes[nodeId];
            const nodeElement = document.createElement("div");  // Create a new div for each node
            nodeElement.textContent = `Node ==> ${nodeId}: ${status}`;  // Display node status
            monitorElement.appendChild(nodeElement);  // Append node element to the monitor
        }
    }

    // Function to update the cluster status based on all node statuses
    function updateClusterStatus(nodes, buttonElement) {
        const nodeStatuses = Object.values(nodes);  // Get an array of all node statuses

        if (nodeStatuses.length === 0) {
            // If there are no nodes, set the status to OFF
            clusterStatus.textContent = "OFF";
            updateButtonText("Start Cluster");  // Update the button text
        } else if (nodeStatuses.includes("BOOTING")) {
            // If any node is BOOTING, set the status to "BOOTING UP!"
            clusterStatus.textContent = "BOOTING UP!";
            updateButtonText("Restart Cluster");  // Update the button text
        } else if (nodeStatuses.every(status => status === "RUNNING")) {
            // If all nodes are RUNNING, set the status to "ON"
            clusterStatus.textContent = "ON";
            updateButtonText("Restart Cluster");  // Update the button text
        }
    }

    // Helper function to update the button text
    function updateButtonText(text) {
        buttonElement.textContent = text;  // Update the button label
    }
}

function startCluster() {
    const buttonElement = document.querySelector('button');
    const messageElement = document.getElementById('response-message');

    // Start the cluster process
    startClusterProcess(buttonElement, messageElement);
}

function startClusterProcess(buttonElement, messageElement) {
    // Clear any previous message or loader
    messageElement.textContent = '';
    
    // Reset the message color to default (black or your preferred color)
    messageElement.style.color = "black";

    // Display the "Cluster starting up" message and the loader
    const loaderMessage = document.createElement('span');
    loaderMessage.textContent = 'Loading';
    const loader = document.createElement('span');
    loader.className = 'loader';
    loader.textContent = ' / '; // Initial loader symbol

    // Append the loader to the message element
    messageElement.appendChild(loaderMessage);
    messageElement.appendChild(loader);

    // Animate the loader
    const symbols = ['/', '-', '\\', '|'];
    let index = 0;
    const intervalId = setInterval(() => {
        loader.textContent = symbols[index];
        index = (index + 1) % symbols.length;
    }, 140);

    // Call the cluster start API
    fetch('https://cluster.burla.dev/restart_cluster', { method: 'POST' })
        .then(response => {
            if (response.ok) {
                // If the response is OK, do nothing further
                return;
            } else {
                return response.json().then(err => {
                    throw new Error(`Cluster Failed - ${response.status} ${response.statusText}`);
                });
            }
        })
        .catch(error => {
            // Display the error message if the response is not OK
            messageElement.textContent = `Error: ${error.message}`;
            messageElement.style.color = "red";
        })
        .finally(() => {
            // Add a 1-second delay before stopping the loader
            setTimeout(() => {
                clearInterval(intervalId);  // Stop the loader animation
                loader.remove();
                loaderMessage.remove();
                buttonElement.disabled = false; // Re-enable the button
            }, 800);  // 1-second delay
        });
}


// Monitor the cluster status as soon as the page loads
window.onload = function() {
    monitorCluster(); // Start monitoring the cluster status
};
