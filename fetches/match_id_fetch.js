require("dotenv").config(); // Load .env variables
const fs = require("fs");

async function fetchTournamentData(tournamentIds) {
    const baseUrl = "https://api.football.wisesport.com/v1.0/tournaments";
    
    const matchPromises = tournamentIds.map(async (tournamentId) => {
        const matches = await fetchData(`${baseUrl}/${tournamentId}/matches`);
        return matches.matches ? { tournamentId, matches: matches.matches || [] } : [];
    });

    const matches = (await Promise.all(matchPromises)).flat();

    return matches.filter(match => match !== null); // Remove failed requests
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
        "match_id", 
        "home_team",
        "away_team"
    ];
    
    let csv = headers.join(",") + "\n";

    data.forEach(({tournamentId, matches}) => {

        matches.forEach(match => {
            const tournament_id = tournamentId;
            const match_id = match.id  || "";
            const homeTeam = match.homeTeam?.fullName  || "";
            const awayTeam = match.awayTeam?.fullName  || "";

            csv += `${tournament_id},${match_id},${homeTeam},${awayTeam}\n`;
        });
    });

    return csv;
}

function saveCSVFile(csvData, filename) {
    fs.writeFileSync(filename, csvData);
    console.log(`CSV file saved as: ${filename}`);
}


fs.readFile('./data/extracted_tournament_ids.json', 'utf8', (err, data) => {
    if (err) throw err;
    const extracted_tournament_ids = JSON.parse(data);
    fetchTournamentData(extracted_tournament_ids).then(data => {
        const csv = convertToCSV(data);
        saveCSVFile(csv, "data/match_ids.csv");
    });
});