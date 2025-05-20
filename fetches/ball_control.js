require("dotenv").config(); // Load .env variables
const fs = require("fs");
const pLimit = require("p-limit").default;
const limit = pLimit(1);


async function fetchControlStatistics(tournamentIds) {
    const baseUrl = "https://api.football.wisesport.com/v1.0/tournaments";

    const matchPromises = tournamentIds.map(tournamentId =>
        limit(async () => {
            const matches = await fetchData(`${baseUrl}/${tournamentId}/matches`);
            return matches.matches ? matches.matches.map(match => ({ tournamentId, matchId: match.id })) : [];
    }));

    const matches = (await Promise.all(matchPromises)).flat();

    const controlstatsPromises = matches.map(({ tournamentId, matchId }) =>
        limit(async () => {
            const controlstats = await fetchData(
                `${baseUrl}/${tournamentId}/matches/${matchId}/ballcontrol`
            );
            return controlstats ? { tournamentId, matchId, controls: controlstats.periodBallControlStatistics || [] } : null;
    }));

    const controlStats = await Promise.all(controlstatsPromises);
    return controlStats.filter(stat => stat !== null); // Remove failed requests
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

function convertPlayerStatsToCSV(data) {
    const headers = [
        "tournament_id", "match_id", "period",
        "homeTeamControlDuration", "awayTeamControlDuration",
        "contestedControlDuration", "looseDuration"
    ];

    const rows = data.map(({ tournamentId, matchId, controls }) => {
        return controls.map(period => {
            const homeTeamControlDuration = period.totalBallControlStatistics.homeTeamControlDuration;
            const awayTeamControlDuration = period.totalBallControlStatistics.awayTeamControlDuration;
            const contestedControlDuration = period.totalBallControlStatistics.contestedControlDuration;
            const looseDuration = period.totalBallControlStatistics.looseDuration;
            return [
                tournamentId, matchId, period.period,
                homeTeamControlDuration, awayTeamControlDuration,
                contestedControlDuration, looseDuration
            ];
        });
    }).flat();

    const csv = [headers, ...rows].map(row => row.join(",")).join("\n");
    
    return csv;

}

function saveCSVFile(csvData, filename) {
    fs.writeFileSync(filename, csvData);
    console.log(`CSV file saved as: ${filename}`);
}

fs.readFile('./data/extracted_tournament_ids.json', 'utf8', (err, data) => {
    if (err) throw err;
    const extracted_tournament_ids = JSON.parse(data);
    fetchControlStatistics(extracted_tournament_ids).then(data => {
        const csv = convertPlayerStatsToCSV(data);
        saveCSVFile(csv, "data/control_statistics.csv");
    });
});
