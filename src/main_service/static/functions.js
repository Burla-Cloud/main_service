// let clusterIsOn = false; // Track cluster status globally
// let eventSource; // Reuse the EventSource globally

// function watchCluster() {
//     const nodesElement = document.getElementById('monitor-message');
//     const clusterElement = document.getElementById('cluster-status');
//     const restartButton = document.querySelector('button');
//     let eventSource = new EventSource('/v1/cluster');
//     let nodes = {};

//     console.log("HERE")
//     clusterElement.textContent = "OFF";
//     restartButton.textContent = "Start Cluster";

//     eventSource.onmessage = function(event) {
//         const data = JSON.parse(event.data);
//         const { nodeId, status, deleted } = data;

//         if (status) {
//             nodes[nodeId] = status;
//         } else if (deleted) {
//             delete nodes[nodeId];
//         }
        
//         updateNodesStatus(nodes);
//         updateClusterStatus(nodes, restartButton);
//     };

//     eventSource.onerror = function(error) {
//         nodesElement.innerHTML = "";
//         nodesElement.textContent = "Error: Unable to receive updates.";
//         eventSource.close();
//     };

//     function updateNodesStatus(nodes) {
//         nodesElement.innerHTML = "";

//         for (const nodeId in nodes) {
//             const status = nodes[nodeId];
//             const nodeElement = document.createElement("div");
//             nodeElement.textContent = `Node ${nodeId} is ${status}`;
//             nodesElement.appendChild(nodeElement);
//         }
//     }

//     function updateClusterStatus(nodes, restartButton) {
//         const nodeStatuses = Object.values(nodes);

//         if (nodeStatuses.length === 0) {
//             clusterElement.textContent = "OFF";
//             restartButton.textContent = "Start Cluster";

//         } else if (nodeStatuses.includes("BOOTING")) {
//             clusterElement.textContent = "BOOTING";
//             restartButton.textContent = "Restart Cluster";

//         } else if (nodeStatuses.every(status => status === "READY")) {
//             clusterElement.textContent = "ON";
//             restartButton.textContent = "Restart Cluster";
//         }
//     }
// }

// function startCluster() {
//     const restartButton = document.getElementById('start-button');;
//     const messageElement = document.getElementById('response-message');

//     // Clear any previous message or loader
//     messageElement.textContent = '';
    
//     // Reset the message color to default (black or your preferred color)
//     messageElement.style.color = "black";

//     // Display the "Cluster starting up" message and the loader
//     const loaderMessage = document.createElement('span');
//     loaderMessage.textContent = 'Loading';
//     const loader = document.createElement('span');
//     loader.className = 'loader';
//     loader.textContent = ' / '; // Initial loader symbol

//     // Append the loader to the message element
//     messageElement.appendChild(loaderMessage);
//     messageElement.appendChild(loader);

//     // Animate the loader
//     const symbols = ['/', '-', '\\', '|'];
//     let index = 0;
//     const intervalId = setInterval(() => {
//         loader.textContent = symbols[index];
//         index = (index + 1) % symbols.length;
//     }, 140);

//     const isLocalhost = window.location.hostname === 'localhost';
//     const baseUrl = isLocalhost 
//         ? 'http://localhost:5001'  // Development environment
//         : 'https://cluster.burla.dev';  // Production environment

//     // Call the cluster start API
//     fetch(`${baseUrl}/v1/cluster/restart`, { method: 'POST' })
//         .then(response => {
//             if (response.ok) {
//                 // If the response is OK, do nothing further
//                 return;
//             } else {
//                 return response.json().then(err => {
//                     throw new Error(`Cluster Failed - ${response.status} ${response.statusText}`);
//                 });
//             }
//         })
//         .catch(error => {
//             // Display the error message if the response is not OK
//             messageElement.textContent = `Error: ${error.message}`;
//             messageElement.style.color = "red";
//         })
//         .finally(() => {
//             // Add a 1-second delay before stopping the loader
//             setTimeout(() => {
//                 clearInterval(intervalId);  // Stop the loader animation
//                 loader.remove();
//                 loaderMessage.remove();
//                 restartButton.disabled = false; // Re-enable the button
//             }, 800);  // 1-second delay
//         });
// }

// function shutdownCluster() {
//     const shutdownButton = document.getElementById('shutdown-button');
//     const messageElement = document.getElementById('response-message');

//     // Clear any previous message or loader
//     messageElement.textContent = '';
//     messageElement.style.color = "black";

//     // Display the "Shutting down" message and the loader
//     const loaderSymbols = ['/', '-', '\\', '|'];
//     let loaderIndex = 0;

//     function updateLoader() {
//         messageElement.textContent = 'Shutting Down ' + loaderSymbols[loaderIndex];
//         loaderIndex = (loaderIndex + 1) % loaderSymbols.length;
//     }

//     updateLoader();
//     const intervalId = setInterval(updateLoader, 140);

//     const isLocalhost = window.location.hostname === 'localhost';
//     const baseUrl = isLocalhost 
//         ? 'http://localhost:5001'  // Development environment
//         : 'https://cluster.burla.dev';  // Production environment

//     // Disable the button to prevent multiple clicks
//     shutdownButton.disabled = true;

//     // Send the shutdown request
//     fetch(`${baseUrl}/v1/cluster/shutdown`, {
//         method: 'POST',
//         headers: {
//             'Content-Type': 'application/json',
//         },
//     })
//     .then(response => {
//         if (response.ok) {
//             console.log('Cluster shutdown response:', response.statusText);
//             // Display a success message
//             messageElement.textContent = 'Cluster shutdown initiated successfully';
//         } else {
//             return response.json().then(err => {
//                 throw new Error(`Failed to shut down cluster - ${response.status} ${response.statusText}`);
//             });
//         }
//     })
//     .catch(error => {
//         console.error('Error shutting down cluster:', error);
//         messageElement.textContent = `Error: ${error.message}`;
//         messageElement.style.color = "red";
//     })
//     .finally(() => {
//         // Add a small delay before stopping the loader
//         setTimeout(() => {
//             clearInterval(intervalId);  // Stop the loader animation
//             shutdownButton.disabled = false; // Re-enable the button
//         }, 800);  // 0.8-second delay
//     });
// }

// window.onload = function() {
//     watchCluster();
// };

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
