// Global variables to track cluster state and elements
let clusterIsOn = false;
let eventSource;
let isStartingUp = false;
let isShuttingDown = false;
let messageElement = null;
let startButton = null;
let shutdownButton = null;
let shutdownIntervalId = null;
let startIntervalId = null;
let clusterElement = null;

function watchCluster() {
    const nodesElement = document.getElementById('monitor-message');
    clusterElement = document.getElementById('cluster-status');
    let eventSource = new EventSource('/v1/cluster');
    let nodes = {};

    console.log("HERE");
    clusterElement.textContent = "OFF";
    startButton.textContent = "Start Cluster";
    // No need to disable the shutdown button here since it's handled in window.onload

    eventSource.onmessage = function(event) {
        const data = JSON.parse(event.data);
        const { nodeId, status, deleted } = data;

        if (status) {
            nodes[nodeId] = status;
        } else if (deleted) {
            delete nodes[nodeId];
        }

        updateNodesStatus(nodes);
        updateClusterStatus(nodes);
    };

    eventSource.onerror = function(error) {
        nodesElement.innerHTML = "";
        nodesElement.textContent = "Error: Unable to receive updates.";
        eventSource.close();
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

    function updateClusterStatus(nodes) {
        const nodeStatuses = Object.values(nodes);

        if (nodeStatuses.length === 0) {
            clusterElement.textContent = "OFF";
            startButton.textContent = "Start Cluster";
            // If we were shutting down, clear the message and stop the loader
            if (isShuttingDown) {
                isShuttingDown = false;
                messageElement.textContent = '';
                if (shutdownIntervalId) {
                    clearInterval(shutdownIntervalId);
                    shutdownIntervalId = null;
                }
            }
            // Adjust button states
            startButton.disabled = false;
            shutdownButton.disabled = true;

        } else if (nodeStatuses.includes("BOOTING")) {
            clusterElement.textContent = "BOOTING";
            startButton.textContent = "Restart Cluster";
            // Disable both buttons during booting
            startButton.disabled = true;
            shutdownButton.disabled = true;

        } else if (nodeStatuses.every(status => status === "READY")) {
            clusterElement.textContent = "ON";
            startButton.textContent = "Restart Cluster";
            // If we were starting up, clear the message and stop the loader
            if (isStartingUp) {
                isStartingUp = false;
                messageElement.textContent = '';
                if (startIntervalId) {
                    clearInterval(startIntervalId);
                    startIntervalId = null;
                }
            }
            // Adjust button states
            startButton.disabled = false;
            shutdownButton.disabled = false;
        }
    }
}

function startCluster() {
    // Clear any previous message or loader
    messageElement.textContent = '';
    messageElement.style.color = "black";

    // Display the "Starting Cluster" message and the loader
    const loaderSymbols = ['/', '-', '\\', '|'];
    let loaderIndex = 0;

    function updateLoader() {
        messageElement.textContent = 'Loading\u00A0\u00A0' + loaderSymbols[loaderIndex];
        loaderIndex = (loaderIndex + 1) % loaderSymbols.length;
    }

    updateLoader();
    const intervalId = setInterval(updateLoader, 154); // Slowed down by 10%
    startIntervalId = intervalId;

    const isLocalhost = window.location.hostname === 'localhost';
    const baseUrl = isLocalhost
        ? 'http://localhost:5001'  // Development environment
        : 'https://cluster.burla.dev';  // Production environment

    // Disable the start button and shutdown button
    startButton.disabled = true;
    shutdownButton.disabled = true;

    isStartingUp = true;

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
            isStartingUp = false;
            // Stop loader animation
            clearInterval(startIntervalId);
            startIntervalId = null;
        });
}

function shutdownCluster() {
    // Clear any previous message or loader
    messageElement.textContent = '';
    messageElement.style.color = "black";

    // Display the "Shutting Down" message and the loader
    const loaderSymbols = ['/', '-', '\\', '|'];
    let loaderIndex = 0;

    function updateLoader() {
        messageElement.textContent = 'Shutting Down\u00A0\u00A0' + loaderSymbols[loaderIndex];
        loaderIndex = (loaderIndex + 1) % loaderSymbols.length;
    }

    updateLoader();
    const intervalId = setInterval(updateLoader, 154); // Slowed down by 10%
    shutdownIntervalId = intervalId;

    const isLocalhost = window.location.hostname === 'localhost';
    const baseUrl = isLocalhost
        ? 'http://localhost:5001'  // Development environment
        : 'https://cluster.burla.dev';  // Production environment

    // Disable both buttons
    shutdownButton.disabled = true;
    startButton.disabled = true;

    isShuttingDown = true;

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
            isShuttingDown = false;
            // Stop loader animation
            clearInterval(shutdownIntervalId);
            shutdownIntervalId = null;
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
};
