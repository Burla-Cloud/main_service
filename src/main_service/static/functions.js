// function startCluster() {
//     fetch('http://0.0.0.0:5001/cluster', {
//         method: 'POST'
//     })
//     .then(response => {
//         if (response.ok) {
//             return response.json();
//         } else {
//             throw new Error('Request failed');
//         }
//     })
//     .then(data => {
//         // Display success message in the HTML
//         const messageElement = document.getElementById('response-message');
//         messageElement.textContent =  data.message;
//         messageElement.style.color = 'black';

//         // Clear the success message after 5 seconds
//         setTimeout(() => {
//             messageElement.textContent = '';
//         }, 5000); // 5000 milliseconds = 5 seconds
//     })
//     .catch(error => {
//         // Alert the error message
//         alert('Error: ' + error.message);
//     });
// }


// function startCluster() {
//     // Display "cluster starting up" message
//     const messageElement = document.getElementById('response-message');
//     messageElement.textContent = 'Cluster starting up...';
//     messageElement.style.color = 'black';  // You can change the color as needed

//       // Create the loader element
//       const loader = document.createElement('div');
//       loader.className = 'loader';
//       loader.textContent = '/'; // Initial content for the loader
//       messageElement.appendChild(loader); // Add the loader to the message element


//     fetch('http://0.0.0.0:5001/cluster', {
//         method: 'POST'
//     })
//     .then(response => {
//         if (response.ok) {
//             return response.json();
//         } else {
//             // Attempt to parse and return the error message from the response
//             return response.json().then(err => {
//                 throw new Error(err.detail || 'Request failed');
//             });
//         }
//     })
//     .then(data => {
//         // Update the message with the success response
//         messageElement.textContent = data.message;
//         messageElement.style.color = 'black';

//         // Clear the success message after 5 seconds
//         setTimeout(() => {
//             messageElement.textContent = '';
//         }, 5000); // 5000 milliseconds = 5 seconds
//     })
//     .catch(error => {
//         // Update the message with the error response from the server
//         messageElement.textContent = 'Error: ' + error.message;
//         messageElement.style.color = 'red';
//     });
// }


function startCluster() {
    const buttonElement = document.querySelector('button'); // Select the button
    const messageElement = document.getElementById('response-message');
    
    // Disable the button to prevent multiple clicks
    buttonElement.disabled = true;
    
    // Display "Cluster starting up" message
    messageElement.textContent = 'Cluster starting up';
    messageElement.style.color = 'black';

    // Create the loader element
    const loader = document.createElement('span'); // Use <span> to keep it inline
    loader.className = 'loader';
    loader.textContent = '/'; // Initial content for the loader
    messageElement.appendChild(loader); // Add the loader to the message element

    // Function to rotate the loader content
    const symbols = ['/', '-', '\\', '|'];
    let index = 0;
    const intervalId = setInterval(() => {
        loader.textContent = symbols[index];
        index = (index + 1) % symbols.length;
    }, 140); // Adjust the speed of the spinning effect here

    fetch('http://0.0.0.0:5001/cluster', {
        method: 'POST'
    })
    .then(response => {
        if (response.ok) {
            return response.text(); // Get the plain text response ("Success")
        } else {
            // If the response is not OK, return the error message with status
            return response.json().then(err => {
                // Format the error message as "Cluster Failed - 500 Internal Server Error"
                throw new Error(`${err.detail} - ${response.status} ${response.statusText}`);
            });
        }
    })
    .then(data => {
        // Update the message with the success response
        messageElement.textContent = 'Cluster On'; // Display "Cluster On" on success
        messageElement.style.color = 'black'; // Ensure "Cluster On" is black

        // Stop the spinner after updating the message
        clearInterval(intervalId); // Stop the spinning effect

        // Clear the message after 5 seconds and re-enable the button
        setTimeout(() => {
            messageElement.textContent = ''; // Clear the message
            buttonElement.disabled = false; // Re-enable the button after the message clears
        }, 5000); // 5000 milliseconds = 5 seconds
    })
    .catch(error => {
        // Display the formatted error message
        messageElement.textContent = error.message;
        messageElement.style.color = 'red'; // Change color to red for errors
        clearInterval(intervalId); // Stop the spinning effect
        buttonElement.disabled = false; // Re-enable the button after displaying the error
    });
}
