require("dotenv").config(); // Load .env variables
const fs = require("fs");
const pLimit = require("p-limit").default;
const limit = pLimit(5);

// sleep function
function sleep(time) {
    return new Promise((resolve) => setTimeout(resolve, time));
}

async function fetchTournamentData(tournamentIds) {
    const baseUrl = "https://api.football.wisesport.com/v1.0/tournaments";

    const matchPromises = tournamentIds.map(tournamentId => 
        limit(async () => {
            const matches = await fetchData(`${baseUrl}/${tournamentId}/matches`);
            return matches.matches ? matches.matches.map(match => ({ tournamentId, matchId: match.id })) : [];
    }));

    const matches = (await Promise.all(matchPromises)).flat();

    const timelinePromises = matches.map(({ tournamentId, matchId }) => 
        limit(async () => {
            const timeline = await fetchData(
            `   ${baseUrl}/${tournamentId}/matches/${matchId}/timeline?eventTypes=Shot,Pass,BallContest,Dribble,HeaderContest`
            );

            // Ensure we attach tournamentId & matchId
            return timeline ? { tournamentId, matchId, events: timeline.events || [] } : null;
    }));

    const timelines = await Promise.all(timelinePromises);
    return timelines.filter(timeline => timeline !== null); // Remove failed requests
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
        "event_type",
        "period",
        "result",
        "team",
        "seconds_from_period_start",
        "start_time", 
        "end_time", 
        "start_position_x", 
        "start_position_y",
        "end_position_x", 
        "end_position_y",
        "speed"
    ];

    let csv = headers.join(",") + "\n";

    data.forEach(({ tournamentId, matchId, events }) => {

        events.forEach(event => {
            const eventType = event.type;
            const tournament_id = tournamentId
            const match_id = matchId
            // Find the key that holds the event data (e.g., "pass", "ballContest", etc.)
            const eventData = Object.values(event).find(value => typeof value === "object" && value !== null);

            if (!eventData) return; // Skip if there's no event data

            // Extract fields dynamically, handling missing values
            const startTime = eventData.startTimestamp || "";
            const endTime = eventData.endTimestamp || "";
            const startPositionX = eventData.startPosition?.x || "";
            const startPositionY = eventData.startPosition?.y || "";
            const endPositionX = eventData.endPosition?.x || "";
            const endPositionY = eventData.endPosition?.y || "";
            const period = eventData.period || "";
            const speed = eventData.speed || "";
            const result = eventData.result || "";
            const team = eventData.team || "";
            const seconds_from_period_start = eventData.secondsFromPeriodStart || "";

            csv += `${tournament_id},${match_id},${eventType},${period},${result},${team},${seconds_from_period_start},${startTime},${endTime},${startPositionX},${startPositionY},${endPositionX},${endPositionY},${speed}\n`;
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
        saveCSVFile(csv, "data/match_timelines.csv");
    });
});

