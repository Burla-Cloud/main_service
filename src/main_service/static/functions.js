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

    fetch('https://cluster.burla.dev/restart_cluster', {
        method: 'POST'
    })
    .then(response => {
        if (response.ok) {
            return response.text(); // Get the plain text response ("Success")
        } else {
            // If the response is not OK, return the error message with status
            return response.json().then(err => {
                // Format the error message as "Cluster Failed - 500 Internal Server Error"
                throw new Error(`Cluster Failed - ${response.status} ${response.statusText}`);
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
