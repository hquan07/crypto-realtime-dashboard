// --- CONFIG & CONSTANTS ---
const currencyFormatter = new Intl.NumberFormat('en-US', {
    style: 'currency', currency: 'USD', minimumFractionDigits: 2, maximumFractionDigits: 4
});
// Compact formatter for large USD volume figures (e.g. $2.4M) used in the Coin Info Modal
const compactCurrencyFormatter = new Intl.NumberFormat('en-US', {
    style: 'currency', currency: 'USD', notation: 'compact', maximumFractionDigits: 2
});

// Cache of the most recent /api/data payload, used by the Coin Info Modal so it doesn't
// need its own network round-trip when a price card is clicked.
window.lastData = null;
let currentModalSymbol = null;

// Dynamic state
let allSymbols = [];
let topSymbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 'DOGEUSDT', 'DOTUSDT', 'LINKUSDT'];
let selectedCoin = 'BTCUSDT'; // For candlestick and specific focus
let previousPrices = {};
let priceHistory = {};
let basePrices = {};
const maxHistoryLength = 60;
let timeLabels = [];

Chart.defaults.color = '#94a3b8';
Chart.defaults.font.family = 'Inter';

function getColorForSymbol(sym) {
    const defaultColors = {
        'BTCUSDT': { bg: 'rgba(245, 158, 11, 0.8)', border: '#f59e0b' },
        'ETHUSDT': { bg: 'rgba(59, 130, 246, 0.8)', border: '#3b82f6' },
        'SOLUSDT': { bg: 'rgba(20, 184, 166, 0.8)', border: '#14b8a6' },
        'ADAUSDT': { bg: 'rgba(16, 185, 129, 0.8)', border: '#10b981' },
        'XRPUSDT': { bg: 'rgba(99, 102, 241, 0.8)', border: '#6366f1' },
        'DOGEUSDT': { bg: 'rgba(234, 179, 8, 0.8)', border: '#eab308' },
        'DOTUSDT': { bg: 'rgba(236, 72, 153, 0.8)', border: '#ec4899' },
        'LINKUSDT': { bg: 'rgba(59, 130, 246, 0.8)', border: '#3b82f6' }
    };
    if (defaultColors[sym]) return defaultColors[sym];
    // Hash string to color
    let hash = 0;
    for (let i = 0; i < sym.length; i++) hash = sym.charCodeAt(i) + ((hash << 5) - hash);
    const c = (hash & 0x00FFFFFF).toString(16).toUpperCase();
    const hex = '#' + "00000".substring(0, 6 - c.length) + c;
    return { bg: hex + 'CC', border: hex };
}

// --- TABS LOGIC ---
document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
        btn.classList.add('active');
        document.getElementById(btn.getAttribute('data-tab')).classList.add('active');
        if (btn.getAttribute('data-tab') === 'tab-pro' && lwChart) {
            setTimeout(() => lwChart.resize(document.getElementById('candlestick-container').clientWidth, 500), 10);
        }
    });
});

// --- FILTER/SEARCH LOGIC ---
document.addEventListener('DOMContentLoaded', () => {
    const coinSearch = document.getElementById('coinSearch');
    if (coinSearch) {
        coinSearch.addEventListener('change', (e) => {
            const val = e.target.value.toUpperCase();
            if (allSymbols.includes(val)) {
                selectedCoin = val;
                const titleEl = document.getElementById('candlestick-coin-title');
                if (titleEl) titleEl.innerText = val;
                // Reset candlestick data on change
                if (candleSeries) candleSeries.setData([]);
                currentCandle = null;
                isCandleChartInitialized = false;
                // Reset price filter to that coin
                const priceFilterEl = document.getElementById('priceTrendFilter');
                if (priceFilterEl) priceFilterEl.value = val;
                currentTrendFilter = val;
            } else if (val === '') {
                const priceFilterEl = document.getElementById('priceTrendFilter');
                if (priceFilterEl) priceFilterEl.value = 'ALL_PCT';
                currentTrendFilter = 'ALL_PCT';
            }
        });
    }

    const priceFilterEl = document.getElementById('priceTrendFilter');
    if (priceFilterEl) {
        priceFilterEl.addEventListener('change', (e) => {
            currentTrendFilter = e.target.value;
            if (currentTrendFilter !== 'ALL_PCT') {
                selectedCoin = currentTrendFilter;
                const titleEl = document.getElementById('candlestick-coin-title');
                if (titleEl) titleEl.innerText = selectedCoin;
                if (candleSeries) candleSeries.setData([]);
                currentCandle = null;
                isCandleChartInitialized = false;
                if (coinSearch) coinSearch.value = selectedCoin;
            } else {
                if (coinSearch) coinSearch.value = "";
            }
        });
    }
});
let currentTrendFilter = 'ALL_PCT';

// --- CHARTS INITIALIZATION ---
const volumeChart = new Chart(document.getElementById('volumeChart').getContext('2d'), {
    type: 'bar',
    data: { labels: [], datasets: [{ label: 'Volume (USD)', data: [], borderWidth: 1, borderRadius: 6 }] },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { display: false }, x: { grid: { display: false } } } }
});

const priceChart = new Chart(document.getElementById('priceChart').getContext('2d'), {
    type: 'line',
    data: { labels: timeLabels, datasets: [] },
    options: {
        responsive: true, maintainAspectRatio: false, interaction: { mode: 'index', intersect: false },
        plugins: { legend: { position: 'top' } },
        scales: { y: { type: 'linear', display: true, grid: { color: 'rgba(255, 255, 255, 0.05)' } }, x: { grid: { display: false } } },
        animation: false
    }
});

const doughnutChart = new Chart(document.getElementById('doughnutChart').getContext('2d'), {
    type: 'doughnut',
    data: { labels: [], datasets: [{ data: [], borderWidth: 0, hoverOffset: 10 }] },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'right' } } }
});

const polarChart = new Chart(document.getElementById('polarChart').getContext('2d'), {
    type: 'polarArea',
    data: { labels: [], datasets: [{ data: [], borderWidth: 1, borderColor: '#1e293b' }] },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'right' } } }
});

const bubbleChart = new Chart(document.getElementById('bubbleChart').getContext('2d'), {
    type: 'bubble', data: { datasets: [] },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { type: 'logarithmic', title: { display: true, text: 'Volume' } }, y: { type: 'logarithmic', title: { display: true, text: 'Price' } } } }
});

const radarChart = new Chart(document.getElementById('radarChart').getContext('2d'), {
    type: 'radar', data: { labels: ['Volume', 'Trades', 'Momentum', 'Volatility'], datasets: [] },
    options: { responsive: true, maintainAspectRatio: false, scales: { r: { ticks: { display: false }, grid: { color: 'rgba(255,255,255,0.1)' }, angleLines: { color: 'rgba(255,255,255,0.1)' } } } }
});

const candlestickContainer = document.getElementById('candlestick-container');
let lwChart = LightweightCharts.createChart(candlestickContainer, {
    width: candlestickContainer.clientWidth || 800, height: 500,
    layout: { background: { type: 'solid', color: 'transparent' }, textColor: '#94a3b8' },
    grid: { vertLines: { color: 'rgba(255, 255, 255, 0.05)' }, horzLines: { color: 'rgba(255, 255, 255, 0.05)' } },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    rightPriceScale: { borderColor: 'rgba(255, 255, 255, 0.1)' },
    timeScale: { borderColor: 'rgba(255, 255, 255, 0.1)', timeVisible: true, secondsVisible: false }
});
let candleSeries = lwChart.addSeries(LightweightCharts.CandlestickSeries, { upColor: '#10b981', downColor: '#ef4444', borderVisible: false, wickUpColor: '#10b981', wickDownColor: '#ef4444' });
let smaSeries = lwChart.addSeries(LightweightCharts.LineSeries, { color: '#facc15', lineWidth: 2, crosshairMarkerVisible: false, lastValueVisible: true, priceLineVisible: false });
let currentCandle = null;
let isCandleChartInitialized = false;

window.addEventListener('resize', () => { if (lwChart) lwChart.resize(candlestickContainer.clientWidth, 500); });

// --- DATA FETCHING & UI UPDATE ---
async function fetchDashboardData() {
    try {
        const response = await fetch('/api/data');
        if (!response.ok) throw new Error("API Error");
        const jsonResponse = await response.json();

        if (jsonResponse.status === 'success') {
            document.getElementById('connection-status').innerText = 'Live';
            document.getElementById('connection-status').style.color = '#10b981';
            document.querySelector('.dot').style.backgroundColor = '#10b981';
            updateUI(jsonResponse.data);
        }
    } catch (error) {
        document.getElementById('connection-status').innerText = 'Disconnected';
        document.getElementById('connection-status').style.color = '#ef4444';
        document.querySelector('.dot').style.backgroundColor = '#ef4444';
    }
}

function updateUI(data) {
    try {
        const { prices, volumes, trades, smas } = data;
        if (!prices || !volumes) return;

        window.lastData = data;

        // Update active coins list
        const currentKeys = Object.keys(prices);
        if (currentKeys.length > allSymbols.length) {
            allSymbols = currentKeys;
            document.getElementById('active-coins-count').innerText = `${allSymbols.length} active coins`;
            const datalist = document.getElementById('coins-list');
            datalist.innerHTML = '';
            allSymbols.forEach(sym => {
                const opt = document.createElement('option');
                opt.value = sym;
                datalist.appendChild(opt);
            });
            // Update dropdown
            const filter = document.getElementById('priceTrendFilter');
            filter.innerHTML = '<option value="ALL_PCT">Compare Top 8 (% Change)</option>';
            allSymbols.forEach(sym => {
                const opt = document.createElement('option');
                opt.value = sym;
                opt.innerText = sym;
                filter.appendChild(opt);
            });
            filter.value = currentTrendFilter;
        }


        // Determine top 8 by volume
        const sortedByVol = Object.keys(volumes).sort((a, b) => volumes[b] - volumes[a]);
        if (sortedByVol.length > 0) {
            const newTop = sortedByVol.slice(0, 8);
            const addedSymbols = newTop.filter(x => !topSymbols.includes(x));
            topSymbols = newTop;

            if (addedSymbols.length > 0) {
                // Fetch history for newly added top symbols dynamically
                fetchHistoryData(addedSymbols);
            }
        }


        // If searched coin is not in top 8, append it so we can see its data
        let displaySymbols = [...new Set([...topSymbols, selectedCoin])];
        if (!prices[selectedCoin]) {
            displaySymbols = topSymbols;
        }


        // 1.5 Update AI Prediction Badge
        if (data.predictions && data.predictions[selectedCoin]) {
            const pred = data.predictions[selectedCoin];
            const badge = document.getElementById('ai-badge');
            const msg = document.getElementById('ai-forecast-msg');
            if (badge && msg) {
                badge.style.display = 'block';
                if (pred.direction === 'UPTREND') {
                    badge.style.background = 'rgba(16, 185, 129, 0.2)';
                    badge.style.borderColor = '#10b981';
                    badge.querySelector('.toast-title').style.color = '#10b981';
                    msg.innerText = `UPTREND (${pred.confidence}%)`;
                    msg.style.color = '#10b981';
                } else if (pred.direction === 'DOWNTREND') {
                    badge.style.background = 'rgba(239, 68, 68, 0.2)';
                    badge.style.borderColor = '#ef4444';
                    badge.querySelector('.toast-title').style.color = '#ef4444';
                    msg.innerText = `DOWNTREND (${pred.confidence}%)`;
                    msg.style.color = '#ef4444';
                } else {
                    badge.style.background = 'rgba(148, 163, 184, 0.2)';
                    badge.style.borderColor = '#94a3b8';
                    badge.querySelector('.toast-title').style.color = '#94a3b8';
                    msg.innerText = `SIDEWAYS (${pred.confidence}%)`;
                    msg.style.color = '#94a3b8';
                }
            }
        }

        // 1. Dynamic Price Grid
        const grid = document.getElementById('dynamic-price-grid');
        grid.innerHTML = '';
        displaySymbols.slice(0, 8).forEach(sym => { // Max 8 boxes
            if (!prices[sym]) return;
            const priceEl = document.createElement('div');
            priceEl.className = 'glass-card price-card';
            priceEl.id = `card-${sym}`;
            priceEl.addEventListener('click', () => showCoinModal(sym));

            const curr = prices[sym];
            let trendClass = '';
            if (previousPrices[sym]) {
                if (curr > previousPrices[sym]) trendClass = 'price-up';
                else if (curr < previousPrices[sym]) trendClass = 'price-down';
            }
            previousPrices[sym] = curr;

            const baseCoin = sym.replace('USDT', '').toLowerCase();
            priceEl.innerHTML = `
                <div class="card-info" style="width: 100%; display: flex; align-items: center; gap: 10px;">
                    <img src="https://assets.coincap.io/assets/icons/${baseCoin}@2x.png" 
                         alt="${sym}" 
                         style="width: 40px; height: 40px; border-radius: 50%;"
                         onerror="this.src='https://assets.coincap.io/assets/icons/btc@2x.png'">
                    <div>
                        <h3 style="margin-bottom: 5px;">${sym}</h3>
                        <div class="price ${trendClass}">${currencyFormatter.format(curr)}</div>
                    </div>
                </div>
            `;
            grid.appendChild(priceEl);
        });

        // Setup History
        displaySymbols.forEach(sym => {
            if (!priceHistory[sym]) priceHistory[sym] = [];
            if (!basePrices[sym] && prices[sym]) basePrices[sym] = prices[sym];
            priceHistory[sym].push(prices[sym] || 0);
            if (priceHistory[sym].length > maxHistoryLength) priceHistory[sym].shift();
        });

        const now = new Date();
        timeLabels.push(`${now.getHours()}:${now.getMinutes()}:${now.getSeconds()}`);
        if (timeLabels.length > maxHistoryLength) timeLabels.shift();

        // 2. Overview Charts
        const topLabels = topSymbols.map(s => s.replace('USDT', ''));
        const topColorsBg = topSymbols.map(s => getColorForSymbol(s).bg);
        const topColorsBd = topSymbols.map(s => getColorForSymbol(s).border);

        volumeChart.data.labels = topLabels;
        volumeChart.data.datasets[0].data = topSymbols.map(s => volumes[s] || 0);
        volumeChart.data.datasets[0].backgroundColor = topColorsBg;
        volumeChart.data.datasets[0].borderColor = topColorsBd;
        volumeChart.update('none');

        // Trend Chart
        const chartSyms = currentTrendFilter === 'ALL_PCT' ? topSymbols : [selectedCoin];
        priceChart.data.datasets = chartSyms.map(sym => {
            const rawData = priceHistory[sym] || [];
            let dataArr = rawData;
            if (currentTrendFilter === 'ALL_PCT') {
                const base = basePrices[sym] || 1;
                dataArr = rawData.map(v => ((v - base) / base) * 100);
            }
            return {
                label: sym.replace('USDT', ''),
                data: dataArr,
                borderColor: getColorForSymbol(sym).border,
                borderWidth: 2, pointRadius: 0, tension: 0.4, fill: false
            };
        });

        if (currentTrendFilter === 'ALL_PCT') {
            priceChart.options.scales.y.ticks.callback = function (value) { return value.toFixed(2) + '%'; };
        } else {
            priceChart.options.scales.y.ticks.callback = function (value) { return '$' + value; };
        }
        priceChart.update();

        // 3. Dominance
        doughnutChart.data.labels = topLabels;
        doughnutChart.data.datasets[0].data = topSymbols.map(s => volumes[s] || 0);
        doughnutChart.data.datasets[0].backgroundColor = topColorsBd;
        doughnutChart.update();

        polarChart.data.labels = topLabels;
        polarChart.data.datasets[0].data = topSymbols.map(s => (trades && trades[s]) ? trades[s] : (volumes[s] || 0));
        polarChart.data.datasets[0].backgroundColor = topColorsBg;
        polarChart.update();

        // 4. Activity
        bubbleChart.data.datasets = topSymbols.map(sym => {
            const vol = volumes[sym] || 1;
            const prc = prices[sym] || 1;
            const trd = (trades && trades[sym]) ? trades[sym] : 10;
            return {
                label: sym.replace('USDT', ''),
                data: [{ x: vol, y: prc, r: Math.min(Math.max(trd / 50, 5), 30) }],
                backgroundColor: getColorForSymbol(sym).bg, borderColor: getColorForSymbol(sym).border
            };
        });
        bubbleChart.update();

        radarChart.data.datasets = topSymbols.slice(0, 3).map(sym => {
            return {
                label: sym.replace('USDT', ''),
                data: [
                    Math.log10(volumes[sym] || 1) * 10,
                    Math.log10((trades && trades[sym]) ? trades[sym] : 1) * 10,
                    50 + (Math.random() * 20),
                    30 + (Math.random() * 40)
                ],
                backgroundColor: getColorForSymbol(sym).bg.replace('0.8', '0.3'),
                borderColor: getColorForSymbol(sym).border, borderWidth: 2
            };
        });
        radarChart.update();

        // 5. Candlestick (selectedCoin)
        if (candleSeries && prices[selectedCoin]) {
            const coinPrice = prices[selectedCoin];
            const coeff = 1000 * 60; // 1 min
            const currentMinuteTime = Math.floor(now.getTime() / coeff) * 60;

            if (!isCandleChartInitialized) {
                // Kick off a background fetch of real 1-min klines from Binance.
                // While it's in flight we set an empty series so the chart doesn't
                // show fake data. The Kafka-driven live candle updates below
                // continue independently.
                fetchBinanceKlines(selectedCoin);
                isCandleChartInitialized = true;
            }

            if (!currentCandle || currentCandle.time !== currentMinuteTime) {
                currentCandle = { time: currentMinuteTime, open: coinPrice, high: coinPrice, low: coinPrice, close: coinPrice };
            } else {
                currentCandle.high = Math.max(currentCandle.high, coinPrice);
                currentCandle.low = Math.min(currentCandle.low, coinPrice);
                currentCandle.close = coinPrice;
            }
            candleSeries.update(currentCandle);

            if (smas && smas[selectedCoin] && smaSeries) {
                smaSeries.update({ time: currentMinuteTime, value: smas[selectedCoin] });
            }
        }
    } catch (e) {
        console.error("Error updating UI:", e);
    }
}

// --- COIN INFO MODAL ---

// Approximates % price change over the last ~1 minute using the client-side rolling
// priceHistory buffer (polled every 1s, up to 60 points). This is a client-side estimate,
// not an exact snapshot from the backend, so results can vary slightly depending on how
// long the dashboard has been open / when history was last (re)loaded for that symbol.
function getPriceChangePct(sym, currentPrice) {
    const hist = priceHistory[sym];
    if (!hist || hist.length < 2 || !currentPrice) return null;
    const past = hist[0];
    if (!past) return null;
    return ((currentPrice - past) / past) * 100;
}

function showCoinModal(sym) {
    const data = window.lastData;
    if (!data || !data.prices || !data.prices[sym]) return;

    currentModalSymbol = sym;
    const { prices, volumes, trades, smas, predictions } = data;
    const price = prices[sym];
    const baseCoin = sym.replace('USDT', '').toLowerCase();

    document.getElementById('modal-coin-icon').src = `https://assets.coincap.io/assets/icons/${baseCoin}@2x.png`;
    document.getElementById('modal-coin-symbol').innerText = sym;
    document.getElementById('modal-coin-price').innerText = currencyFormatter.format(price);

    const sma = smas && smas[sym];
    document.getElementById('modal-coin-sma').innerText = sma ? currencyFormatter.format(sma) : '--';

    const changePct = getPriceChangePct(sym, price);
    const changeEl = document.getElementById('modal-coin-change');
    if (changePct === null) {
        changeEl.innerText = '--';
        changeEl.className = 'modal-stat-value';
    } else {
        changeEl.innerText = `${changePct >= 0 ? '+' : ''}${changePct.toFixed(2)}%`;
        changeEl.className = 'modal-stat-value ' + (changePct >= 0 ? 'price-up' : 'price-down');
    }

    document.getElementById('modal-coin-volume').innerText = volumes[sym] ? compactCurrencyFormatter.format(volumes[sym]) : '--';
    document.getElementById('modal-coin-trades').innerText = (trades && trades[sym]) ? Number(trades[sym]).toLocaleString('en-US') : '0';

    const forecastBox = document.getElementById('modal-coin-forecast');
    const forecastVal = document.getElementById('modal-coin-forecast-value');
    const pred = predictions && predictions[sym];
    if (pred) {
        const colors = { UPTREND: '#10b981', DOWNTREND: '#ef4444', SIDEWAYS: '#94a3b8' };
        const c = colors[pred.direction] || '#94a3b8';
        forecastBox.style.borderColor = c;
        forecastBox.style.background = c + '20';
        forecastVal.innerText = `${pred.direction} (${pred.confidence}%)`;
        forecastVal.style.color = c;
    } else {
        forecastBox.style.borderColor = '';
        forecastBox.style.background = '';
        forecastVal.innerText = 'No data yet';
        forecastVal.style.color = 'var(--text-muted)';
    }

    document.getElementById('coin-modal-overlay').classList.add('active');
}

function closeCoinModal() {
    document.getElementById('coin-modal-overlay').classList.remove('active');
    currentModalSymbol = null;
}

function viewCoinCharts() {
    if (!currentModalSymbol) return;
    const sym = currentModalSymbol;
    closeCoinModal();

    // Switch to the Professional tab by reusing its existing click handler
    // (also takes care of resizing the Lightweight Charts candlestick chart).
    const proTabBtn = document.querySelector('.tab-btn[data-tab="tab-pro"]');
    if (proTabBtn) proTabBtn.click();

    // Setting the search box + dispatching 'change' reuses every existing listener
    // (selectedCoin update, candlestick reset, history fetch, orderbook reconnect)
    // instead of duplicating that logic here.
    const coinSearch = document.getElementById('coinSearch');
    if (coinSearch) {
        coinSearch.value = sym;
        coinSearch.dispatchEvent(new Event('change'));
    }
}

document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeCoinModal();
});

// Fetch loop
async function fetchHistoryData(symbols) {
    try {
        const symStr = symbols.join(',');
        const response = await fetch(`/api/history?symbols=${symStr}`);
        if (!response.ok) throw new Error("API History Error");
        const jsonResponse = await response.json();

        if (jsonResponse.status === 'success') {
            const historyData = jsonResponse.data;
            symbols.forEach(sym => {
                if (historyData[sym] && historyData[sym].length > 0) {
                    basePrices[sym] = historyData[sym][0].price;
                    priceHistory[sym] = historyData[sym].map(record => record.price);

                    // Populate timeLabels only once
                    if (sym === symbols[0]) {
                        timeLabels = historyData[sym].map(record => {
                            const d = new Date(record.time);
                            return `${d.getHours()}:${d.getMinutes()}:${d.getSeconds()}`;
                        });
                        priceChart.data.labels = timeLabels;
                    }

                    // For candlesticks/RSI on selectedCoin, we do NOT synthesize OHLC
                    // from Redis avg-price. Real 1-min klines are fetched from
                    // Binance separately via fetchBinanceKlines().
                    if (sym === selectedCoin) {
                        fetchBinanceKlines(sym);
                        isCandleChartInitialized = true;
                    }
                }
            });
            priceChart.update();
        }
    } catch (e) {
        console.error("Failed to fetch history:", e);
    }
}

// --- REAL CANDLESTICK DATA (Binance klines REST) ---
// Pulls the last 60 one-minute klines for `symbol` and hydrates the candlestick
// series + RSI series. This replaces the previous approach that decorated
// Redis avg-price with Math.random() noise, which was misleading.
async function fetchBinanceKlines(symbol, interval = '1m', limit = 60) {
    if (!candleSeries) return;
    try {
        const url = `https://api.binance.com/api/v3/klines?symbol=${encodeURIComponent(symbol)}&interval=${interval}&limit=${limit}`;
        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`Binance klines HTTP ${resp.status}`);
        const rows = await resp.json();
        // Binance kline row: [openTime, open, high, low, close, volume, closeTime, ...]
        const candleData = rows.map(k => ({
            time: Math.floor(k[0] / 1000),
            open: parseFloat(k[1]),
            high: parseFloat(k[2]),
            low: parseFloat(k[3]),
            close: parseFloat(k[4]),
        }));
        // Only apply if user hasn't switched coins mid-request
        if (symbol !== selectedCoin) return;
        candleSeries.setData(candleData);
        currentCandle = candleData[candleData.length - 1] || null;

        // Compute RSI on real closes
        const closes = candleData.map(c => c.close);
        const rsiValues = calculateRSI(closes, 14);
        if (rsiSeries && rsiValues.length > 0) {
            const offset = closes.length - rsiValues.length;
            const rsiChartData = rsiValues.map((v, i) => ({
                time: candleData[i + offset].time,
                value: v,
            }));
            rsiSeries.setData(rsiChartData);
        }
    } catch (e) {
        console.error(`fetchBinanceKlines(${symbol}) failed:`, e);
    }
}

// Ensure history is fetched when selection changes
document.addEventListener('DOMContentLoaded', () => {
    const coinSearch = document.getElementById('coinSearch');
    if (coinSearch) {
        coinSearch.addEventListener('change', (e) => {
            const val = e.target.value.toUpperCase();
            if (allSymbols.includes(val)) {
                fetchHistoryData([val]);
            }
        });
    }
    const priceFilterEl = document.getElementById('priceTrendFilter');
    if (priceFilterEl) {
        priceFilterEl.addEventListener('change', (e) => {
            if (e.target.value !== 'ALL_PCT') {
                fetchHistoryData([e.target.value]);
            }
        });
    }
});

fetchHistoryData(topSymbols).then(() => {
    // Initial one-shot fetch so the UI has data immediately on page load
    // (the SSE stream's first frame is delayed by up to SSE_INTERVAL_SEC).
    fetchDashboardData();
    // Live updates via SSE (server pushes every ~2s). Replaces the previous
    // setInterval(fetchDashboardData, 1000) which polled 5x more often than
    // the underlying Flink windows actually produce new data.
    connectDataStream();
});

let dataSource = null;
function connectDataStream() {
    if (dataSource) dataSource.close();
    dataSource = new EventSource('/api/data/stream');
    dataSource.onmessage = (event) => {
        try {
            const parsed = JSON.parse(event.data);
            if (parsed.status === 'success') {
                document.getElementById('connection-status').innerText = 'Live';
                document.getElementById('connection-status').style.color = '#10b981';
                document.querySelector('.dot').style.backgroundColor = '#10b981';
                updateUI(parsed.data);
            }
        } catch (e) { /* ignore malformed frame */ }
    };
    dataSource.onerror = () => {
        // EventSource auto-reconnects on error with exponential backoff;
        // just flip the status pill so the user sees it.
        document.getElementById('connection-status').innerText = 'Reconnecting';
        document.getElementById('connection-status').style.color = '#ef4444';
        document.querySelector('.dot').style.backgroundColor = '#ef4444';
    };
}


// --- SSE ALERTS LISTENER ---
const alertSource = new EventSource('/api/alerts/stream');
alertSource.onmessage = function (event) {
    try {
        const alertData = JSON.parse(event.data);
        showToast(alertData.alert_type, alertData.message);
    } catch (e) { }
};

function showToast(type, message) {
    const container = document.getElementById('toast-container');
    if (!container) return;
    const toast = document.createElement('div');
    toast.className = 'toast';
    let title = "System Alert";
    if (type === 'WHALE_ALERT') {
        toast.classList.add('whale');
        title = "🐋 WHALE ALERT";
    } else if (type === 'DOWNTREND') {
        toast.classList.add('downtrend');
        title = "📉 DOWNTREND ALERT";
    }
    toast.innerHTML = `<div class="toast-title">${title}</div><div class="toast-message">${message}</div>`;
    container.appendChild(toast);
    setTimeout(() => { if (container.contains(toast)) container.removeChild(toast); }, 5000);
}


// --- ORDERBOOK LOGIC ---
let orderbookWs = null;

function connectOrderbook(symbol) {
    if (orderbookWs) {
        orderbookWs.close();
    }
    const wsUrl = `wss://stream.binance.com:9443/ws/${symbol.toLowerCase()}@depth10@100ms`;
    orderbookWs = new WebSocket(wsUrl);

    orderbookWs.onmessage = (event) => {
        const data = JSON.parse(event.data);
        if (data.bids && data.asks) {
            updateOrderbookUI(data.bids, data.asks);
        }
    };
}

function updateOrderbookUI(bids, asks) {
    const bidsContainer = document.getElementById('ob-bids');
    const asksContainer = document.getElementById('ob-asks');
    if (!bidsContainer || !asksContainer) return;

    // Asks (Red)
    asksContainer.innerHTML = '';
    asks.slice(0, 10).forEach(ask => {
        const row = document.createElement('div');
        row.style.display = 'flex';
        row.style.justifyContent = 'space-between';
        row.innerHTML = `<span>${parseFloat(ask[0]).toFixed(4)}</span><span>${parseFloat(ask[1]).toFixed(4)}</span>`;
        asksContainer.appendChild(row);
    });

    // Bids (Green)
    bidsContainer.innerHTML = '';
    bids.slice(0, 10).forEach(bid => {
        const row = document.createElement('div');
        row.style.display = 'flex';
        row.style.justifyContent = 'space-between';
        row.innerHTML = `<span>${parseFloat(bid[0]).toFixed(4)}</span><span>${parseFloat(bid[1]).toFixed(4)}</span>`;
        bidsContainer.appendChild(row);
    });
}

// Ensure Orderbook updates when coin changes
document.addEventListener('DOMContentLoaded', () => {
    connectOrderbook(selectedCoin);

    const coinSearch = document.getElementById('coinSearch');
    if (coinSearch) {
        coinSearch.addEventListener('change', (e) => {
            const val = e.target.value.toUpperCase();
            if (allSymbols.includes(val)) {
                connectOrderbook(val);
            }
        });
    }
    const priceFilterEl = document.getElementById('priceTrendFilter');
    if (priceFilterEl) {
        priceFilterEl.addEventListener('change', (e) => {
            if (e.target.value !== 'ALL_PCT') {
                connectOrderbook(e.target.value);
            }
        });
    }
});

// --- RSI INDICATOR LOGIC ---
let rsiSeries = null;

function calculateRSI(prices, period = 14) {
    if (prices.length <= period) return [];
    let gains = 0, losses = 0;

    for (let i = 1; i <= period; i++) {
        let diff = prices[i] - prices[i - 1];
        if (diff > 0) gains += diff;
        else losses -= diff;
    }

    let avgGain = gains / period;
    let avgLoss = losses / period;
    let rsiData = [];

    for (let i = period; i < prices.length; i++) {
        let rs = avgGain / (avgLoss === 0 ? 1 : avgLoss);
        let rsi = 100 - (100 / (1 + rs));
        rsiData.push(rsi);

        if (i < prices.length - 1) {
            let diff = prices[i + 1] - prices[i];
            let currentGain = diff > 0 ? diff : 0;
            let currentLoss = diff < 0 ? -diff : 0;
            avgGain = ((avgGain * (period - 1)) + currentGain) / period;
            avgLoss = ((avgLoss * (period - 1)) + currentLoss) / period;
        }
    }
    return rsiData;
}

// Add RSI Series to Lightweight Charts
if (lwChart) {
    rsiSeries = lwChart.addLineSeries({
        color: '#a855f7',
        lineWidth: 2,
        priceFormat: { type: 'price', precision: 2, minMove: 0.01 },
        priceScaleId: 'left' // RSI on the left scale, Candlesticks on the right
    });
    lwChart.priceScale('left').applyOptions({
        autoScale: false,
        scaleMargins: { top: 0.8, bottom: 0 },
    });
    lwChart.priceScale('right').applyOptions({
        scaleMargins: { top: 0, bottom: 0.25 },
    });
}