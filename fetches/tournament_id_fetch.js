require("dotenv").config(); // Load .env variables
const fs = require("fs");

async function fetchTournamentData() {
    const baseUrl = "https://api.football.wisesport.com/v1.0/tournaments";
    
    const tournaments = await fetchData(`${baseUrl}`);
    return tournaments.tournaments ? {tournaments: tournaments.tournaments || [] } : [];
}


async function fetchData(url) {
    try {
        const response = await fetch(url, {
            headers: {
                "Wisesport-Api-Key": `${process.env.API_KEY}`
            }
        });

        if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status}`);
        }

        const text = await response.text();
        if (!text) {
            throw new Error("Empty response received");
        }

        return JSON.parse(text);
    } catch (error) {
        console.error(`Error fetching ${url}:`, error);
        return null;
    }
}

function convertToCSV(data) {
    const headers = [
        "tournament_id", 
        "name"
    ];
    
    let csv = headers.join(",") + "\n";


    data.tournaments.map(tournament => {
        const tournament_id = tournament.id || "";
        const name = tournament.name || "";

        csv += `${tournament_id},${name}\n`;
    });

    return csv;
}

function saveCSVFile(csvData, filename) {
    fs.writeFileSync(filename, csvData);
    console.log(`CSV file saved as: ${filename}`);
}


// Example usage
fetchTournamentData().then(data => {
    const csv = convertToCSV(data);
    saveCSVFile(csv, "data/tournament_ids.csv");
});