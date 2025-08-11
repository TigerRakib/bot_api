const previousPrices = {}; // store last prices
let refreshSeconds = 30;  // countdown starting

async function loadSignals() {
    try {
        const response = await fetch("/api/signals");
        const data = await response.json();

        // Update counters
        updateCounters(data);
        
        const tbody = document.querySelector("#signals-table tbody");
        tbody.innerHTML = "";

        data.forEach((row, i) => {
            let direction = null;

            // Compare with previous price to determine up/down
            if (previousPrices[row.symbol] !== undefined) {
                if (row.current_price > previousPrices[row.symbol]) {
                    direction = "up";
                } else {
                    direction = "down";
                }
            }

            // Save current price
            previousPrices[row.symbol] = row.current_price;

            // arrow and color
            const arrow = direction === "up" ? "▲" : (direction === "down" ? "▼" : "");
            const colorClass = direction === "up" ? "price-up" : (direction === "down" ? "price-down" : "");
            const formatDateTime = (dateStr) => {
                const date = new Date(dateStr);
                const pad = (num) => num.toString().padStart(2, '0');

                const year = date.getUTCFullYear();
                const month = pad(date.getUTCMonth() + 1);
                const day = pad(date.getUTCDate());
                const hours = pad(date.getUTCHours());
                const minutes = pad(date.getUTCMinutes());
                const seconds = pad(date.getUTCSeconds());

                return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
                }

            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td>${i + 1}.</td>
                <td>${row.symbol}</td>
                <td>${row.signal_type}</td>
                <td class="${colorClass}">${row.current_price} ${arrow}</td>
                <td>${formatDateTime(row.signal_update_time)}</td>
            `;
            tbody.appendChild(tr);
        });


        

    } catch (error) {
        console.error("Error loading signals:", error);
    }
}

function updateCountdown() {
    const countdownElem = document.getElementById('countdown');
    if (refreshSeconds <= 0) {
        refreshSeconds = 30; 
        loadSignals();        
    }
    countdownElem.textContent = `0:${refreshSeconds.toString().padStart(2, '0')}`;
    refreshSeconds--;
}

function updateCurrentTime() {
  const timeElem = document.getElementById('server-time');
  const now = new Date();

  const year = now.getUTCFullYear();
  const month = String(now.getUTCMonth() + 1).padStart(2, '0');
  const day = String(now.getUTCDate()).padStart(2, '0');
  const hours = String(now.getUTCHours()).padStart(2, '0');
  const minutes = String(now.getUTCMinutes()).padStart(2, '0');
  const seconds = String(now.getUTCSeconds()).padStart(2, '0');
  const milliseconds = String(now.getUTCMilliseconds()).padStart(3, '0');

  const timeStr = `${year}-${month}-${day} ${hours}:${minutes}:${seconds}.${milliseconds}`;
  timeElem.textContent = timeStr;
}

function updateCounters(data) {
    let buyCount = 0;
    let sellCount = 0;
    let holdCount = 0;
    let exitCount = 0;

    data.forEach(row => {
        const signal = row.signal_type?.toLowerCase();
        if (signal === "buy") buyCount++;
        else if (signal === "sell") sellCount++;
        else if (signal === "hold") holdCount++;
        else if (signal === "exit") exitCount++;
    });

    const totalAssets = 245; //  total assests

    // Update counter elements
    document.getElementById("buy-count").textContent = buyCount;
    document.getElementById("sell-count").textContent = sellCount;
    document.getElementById("hold-count").textContent = holdCount;
    document.getElementById("exit-count").textContent = exitCount;

    document.getElementById("total-assets-buy-sell").textContent = totalAssets;
    document.getElementById("total-assets-hold-exit").textContent = totalAssets;
}

// countdown and clock updates
setInterval(() => {
    updateCountdown();
    updateCurrentTime();
}, 1000);

// Initial call 
loadSignals();
updateCountdown();
updateCurrentTime();
