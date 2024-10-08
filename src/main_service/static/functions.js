// Global variables to track cluster state and elements
let nodeEventSource;
let clusterStatusEventSource;
let messageElement = null;
let startButton = null;
let shutdownButton = null;
let loaderIntervalId = null;  // For managing the loader interval
let clusterElement = null;

function watchCluster() {
    const nodesElement = document.getElementById('monitor-message');
    clusterElement = document.getElementById('cluster-status');

    // Initialize the EventSource for node statuses
    if (typeof(EventSource) !== "undefined") {
        nodeEventSource = new EventSource('/v1/cluster');
    } else {
        messageElement.textContent = "Your browser does not support server-sent events.";
        return;  // Stop execution if EventSource isn't supported
    }

    let nodes = {};

    console.log("Initializing node status monitoring");
    clusterElement.textContent = "OFF";
    startButton.textContent = "Start Cluster";

    nodeEventSource.onmessage = function(event) {
        const data = JSON.parse(event.data);
        const { nodeId, status, deleted } = data;

        if (status) {
            nodes[nodeId] = status;
        } else if (deleted) {
            delete nodes[nodeId];
        }

        updateNodesStatus(nodes);
        // No need to call updateClusterStatus here
    };

    nodeEventSource.onerror = function(error) {
        nodesElement.innerHTML = "";
        nodesElement.textContent = "Error: Unable to receive node updates.";
        nodeEventSource.close();
    };

    function updateNodesStatus(nodes) {
        nodesElement.innerHTML = "";

        for (const nodeId in nodes) {
            const status = nodes[nodeId];
            const nodeElement = document.createElement("div");
            nodeElement.textContent = `Node ${nodeId} is ${status}`;
            nodesElement.appendChild(nodeElement);
        }
    }
}

function watchClusterStatus() {
    clusterElement = document.getElementById('cluster-status');

    function createEventSource() {
        console.log("Creating new EventSource connection...");
        
        let source = new EventSource('/v1/cluster/status');
        
        source.onmessage = function(event) {
            const data = JSON.parse(event.data);
            const status = data.status;
            updateClusterStatus(status);
        };
        
        source.onerror = function(error) {
            console.error("Error receiving cluster status updates.", error);
            source.close();  // Close the connection to prevent memory leaks or issues.
            // Retry connection after 2 seconds
            setTimeout(createEventSource, 2000);
        };
    }

    createEventSource();  // Initial connection
}


function updateClusterStatus(status) {
    console.log(`Received cluster status: ${status}`);
    clusterElement.textContent = status;

    if (status === "OFF") {
        // Cluster is OFF
        console.log("Cluster is OFF");
        startButton.textContent = "Start Cluster";
        // Stop loader if running
        stopLoader();
        // Adjust button states
        startButton.disabled = false;
        shutdownButton.disabled = true;
    } else if (status === "BOOTING") {
        // Cluster is BOOTING
        console.log("Cluster is BOOTING");
        startButton.textContent = "Restart Cluster";
        // Start loader if not running
        if (!loaderIntervalId) {
            startLoader('Loading');
        }
        // Disable both buttons during booting
        startButton.disabled = true;
        shutdownButton.disabled = true;
    } else if (status === "ON") {
        // Cluster is ON
        console.log("Cluster is ON");
        startButton.textContent = "Restart Cluster";
        // Stop loader if running
        stopLoader();
        // Adjust button states
        startButton.disabled = false;
        shutdownButton.disabled = false;
    } else if (status === "SHUTTING DOWN") {
        // Cluster is SHUTTING DOWN
        console.log("Cluster is SHUTTING DOWN");
        // Start loader if not running
        if (!loaderIntervalId) {
            startLoader('Loading');
        }
        // Disable both buttons during shutdown
        startButton.disabled = true;
        shutdownButton.disabled = true;
    } else if (status === "ERROR") {
        // Cluster encountered an error
        console.log("Cluster encountered an ERROR");
        // Stop loader if running
        stopLoader();
        // Display error message
        messageElement.textContent = 'Cluster encountered an error.';
        messageElement.style.color = 'red';
        // Adjust button states
        startButton.disabled = false;
        shutdownButton.disabled = true;
    }
}

function startLoader(baseMessage) {
    messageElement.style.color = "black";

    const loaderSymbols = ['/', '-', '\\', '|'];
    let loaderIndex = 0;

    function updateLoader() {
        messageElement.textContent = `${baseMessage}\u00A0\u00A0${loaderSymbols[loaderIndex]}`;
        loaderIndex = (loaderIndex + 1) % loaderSymbols.length;
    }

    updateLoader();
    if (!loaderIntervalId) {
        loaderIntervalId = setInterval(updateLoader, 154);
    }
}

function stopLoader() {
    if (loaderIntervalId) {
        clearInterval(loaderIntervalId);
        loaderIntervalId = null;
        messageElement.textContent = '';  // Clear the message
        console.log("Loader stopped.");
    }
}

function startCluster() {
    // Clear any previous message
    messageElement.textContent = '';
    messageElement.style.color = "black";

    // Disable the start button and shutdown button
    startButton.disabled = true;
    shutdownButton.disabled = true;

    const isLocalhost = window.location.hostname === 'localhost';
    const baseUrl = isLocalhost
        ? 'http://localhost:5001'  // Development environment
        : 'https://cluster.burla.dev';  // Production environment

    // Call the cluster start API
    fetch(`${baseUrl}/v1/cluster/restart`, { method: 'POST' })
        .then(response => {
            if (!response.ok) {
                return response.json().then(err => {
                    throw new Error(`Cluster Failed - ${response.status} ${response.statusText}`);
                });
            }
        })
        .catch(error => {
            // Display the error message
            messageElement.textContent = `Error: ${error.message}`;
            messageElement.style.color = "red";
            // Re-enable buttons
            startButton.disabled = false;
            shutdownButton.disabled = false;
            // Stop loader if it's running
            stopLoader();
        });
}

function shutdownCluster() {
    // Clear any previous message
    messageElement.textContent = '';
    messageElement.style.color = "black";

    // Disable both buttons
    shutdownButton.disabled = true;
    startButton.disabled = true;

    const isLocalhost = window.location.hostname === 'localhost';
    const baseUrl = isLocalhost
        ? 'http://localhost:5001'  // Development environment
        : 'https://cluster.burla.dev';  // Production environment

    // Send the shutdown request
    fetch(`${baseUrl}/v1/cluster/shutdown`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
    })
        .then(response => {
            if (!response.ok) {
                return response.json().then(err => {
                    throw new Error(`Failed to shut down cluster - ${response.status} ${response.statusText}`);
                });
            }
        })
        .catch(error => {
            console.error('Error shutting down cluster:', error);
            messageElement.textContent = `Error: ${error.message}`;
            messageElement.style.color = "red";
            // Re-enable buttons
            shutdownButton.disabled = false;
            startButton.disabled = false;
            // Stop loader if it's running
            stopLoader();
        });
}

window.onload = function() {
    // Initialize global elements
    messageElement = document.getElementById('response-message');
    startButton = document.getElementById('start-button');
    shutdownButton = document.getElementById('shutdown-button');
    clusterElement = document.getElementById('cluster-status');

    // Disable the shutdown button initially
    shutdownButton.disabled = true;

    watchCluster();
    watchClusterStatus();
};
