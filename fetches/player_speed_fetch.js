require("dotenv").config(); // Load .env variables
const fs = require("fs");
const pLimit = require("p-limit").default;
const limit = pLimit(3);


async function fetchPlayerStatistics(tournamentIds) {
    const baseUrl = "https://api.football.wisesport.com/v1.0/tournaments";

    const matchPromises = tournamentIds.map(tournamentId =>
        limit(async () => {
            const matches = await fetchData(`${baseUrl}/${tournamentId}/matches`);
            return matches.matches ? matches.matches.map(match => ({ tournamentId, matchId: match.id })) : [];
    }));

    const matches = (await Promise.all(matchPromises)).flat();

    const statsPromises = matches.map(({ tournamentId, matchId }) =>
        limit(async () => {
            const stats = await fetchData(
                `${baseUrl}/${tournamentId}/matches/${matchId}/playerstatistics`
            );
            return stats ? { tournamentId, matchId, players: stats.playerStatistics || [] } : null;
    }));

    const playerStats = await Promise.all(statsPromises);
    return playerStats.filter(stat => stat !== null); // Remove failed requests
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
        "tournament_id", "match_id", "player_id", "team",
        "top_speed_first", "top_speed_second",
        "avg_speed_with_ball_first", "avg_speed_with_ball_second",
        "time_in_speed_zone_0_5_first", "time_in_speed_zone_0_5_second",
        "time_in_speed_zone_5_10_first", "time_in_speed_zone_5_10_second",
        "time_in_speed_zone_10_15_first", "time_in_speed_zone_10_15_second",
        "time_in_speed_zone_15_20_first", "time_in_speed_zone_15_20_second",
        "time_in_speed_zone_20_25_first", "time_in_speed_zone_20_25_second",
        "time_in_speed_zone_25_plus_first", "time_in_speed_zone_25_plus_second"
    ];

    const rows = data.map(({ tournamentId, matchId, players }) => {
        return players.map(player => {
            const playerId = player.player;
            const team = player.team;
            
            const period1 = player.periodStatistics.find(p => p.period === 1)?.statistics || {};
            const period2 = player.periodStatistics.find(p => p.period === 2)?.statistics || {};

            const speedZones1 = period1.speedZoneStatistics || [];
            const speedZones2 = period2.speedZoneStatistics || [];

            function getSpeedZoneTime(speedZones, minSpeed) {
                return speedZones.find(zone => zone.minSpeed === minSpeed)?.secondsInZone || 0;
            }

            return [
                tournamentId, matchId, playerId, team,
                period1.movementStatistics?.topSpeed || 0,
                period2.movementStatistics?.topSpeed || 0,
                period1.movementStatistics?.averageSpeedWithBall || 0,
                period2.movementStatistics?.averageSpeedWithBall || 0,
                getSpeedZoneTime(speedZones1, 0), getSpeedZoneTime(speedZones2, 0),
                getSpeedZoneTime(speedZones1, 5), getSpeedZoneTime(speedZones2, 5),
                getSpeedZoneTime(speedZones1, 10), getSpeedZoneTime(speedZones2, 10),
                getSpeedZoneTime(speedZones1, 15), getSpeedZoneTime(speedZones2, 15),
                getSpeedZoneTime(speedZones1, 20), getSpeedZoneTime(speedZones2, 20),
                getSpeedZoneTime(speedZones1, 25), getSpeedZoneTime(speedZones2, 25)
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
    fetchPlayerStatistics(extracted_tournament_ids).then(data => {
        const csv = convertPlayerStatsToCSV(data);
        saveCSVFile(csv, "data/player_statistics.csv");
    });
});
