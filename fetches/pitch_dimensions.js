require("dotenv").config(); // Load .env variables
const fs = require("fs");


function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchTournamentData(tournamentIds) {
    const baseUrl = "https://api.football.wisesport.com/v1.0/tournaments";
    let venues = [];

    for (const tournamentId of tournamentIds) {
        // Fetch matches for the tournament
        const matchesData = await fetchData(`${baseUrl}/${tournamentId}/matches`);
        const matches = matchesData.matches || [];

        for (const [index, match] of matches.entries()) {
            await delay(10); // Space out API calls (adjust delay if needed)
            const venue = await fetchData(`${baseUrl}/${tournamentId}/matches/${match.id}/venue`);

            if (venue) {
                venues.push({ tournamentId, matchId: match.id, venue });
            }
        }
    }

    return venues;
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
        "pitch_length", 
        "pitch_width"
    ];

    let csv = headers.join(",") + "\n";

    data.forEach(({ tournamentId, matchId, venue }) => {

        const { pitchLength, pitchWidth } = venue;

        csv += `${tournamentId},${matchId},${pitchLength},${pitchWidth}\n`;
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
        saveCSVFile(csv, "data/pitch_dimensions.csv");
    });
});

