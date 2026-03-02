const app = {
    data: null,
    currentView: 'dashboard',

    init: async function() {
        this.bindEvents();
        await this.fetchData();
        if (this.data) {
            this.renderDashboard();
            this.renderCountriesList();
            this.renderSystemsList();
            this.renderGapsList();

            // Init Map using Google Charts
            if (typeof google !== 'undefined') {
                google.charts.load('current', {
                    'packages':['geochart'],
                });
                google.charts.setOnLoadCallback(() => this.renderMap());
            }
        }
    },

    bindEvents: function() {
        // Navigation clicks
        document.querySelectorAll('.nav-item').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const target = e.currentTarget.dataset.target;
                this.switchView(target);
            });
        });

        const globalSearch = document.getElementById('global-search');
        if (globalSearch) {
            globalSearch.addEventListener('input', (e) => {
                this.handleSearch(e.target.value.toLowerCase(), this.currentView);
            });
        }

        const countrySearch = document.getElementById('country-search');
        if (countrySearch) {
            countrySearch.addEventListener('input', (e) => {
                this.handleSearch(e.target.value.toLowerCase(), 'countries');
            });
        }

        const systemSearch = document.getElementById('system-search');
        if (systemSearch) {
            systemSearch.addEventListener('input', (e) => {
                this.handleSearch(e.target.value.toLowerCase(), 'systems');
            });
        }
    },

    switchView: function(viewId) {
        // Update nav UI
        document.querySelectorAll('.nav-item').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.target === viewId);
        });

        // Update views UI
        document.querySelectorAll('.view').forEach(view => {
            view.classList.toggle('active', view.id === `view-${viewId}`);
        });

        // Update titles
        const titles = {
            'dashboard': 'Global Overview',
            'countries': 'Country Synthesis',
            'systems': 'Production Systems',
            'gaps': 'Evidence Gaps'
        };
        document.getElementById('page-title').textContent = titles[viewId] || 'Dashboard';
        
        this.currentView = viewId;
        
        // Clear searches when switching views
        const searches = ['global-search', 'country-search', 'system-search'];
        searches.forEach(id => {
            const el = document.getElementById(id);
            if(el) el.value = '';
        });
        this.handleSearch('', viewId);
    },

    fetchData: async function() {
        try {
            // Assumes synthesis_report.json is in the same directory served successfully
            const response = await fetch('cgiar_mas_agent2/output/synthesis_report.json');
            if(!response.ok) throw new Error('Network response was not ok');
            this.data = await response.json();
            
            // Format timestamp
            const date = new Date(this.data.generated_at);
            document.getElementById('last-updated').textContent = date.toLocaleDateString(undefined, {
                year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute:'2-digit'
            });

        } catch (error) {
            console.error('Failed to load synthesis report:', error);
            document.getElementById('view-dashboard').innerHTML = `
                <div class="empty-state">
                    <i class="ph ph-warning-circle" style="color: #ef4444;"></i>
                    <p>Failed to load synthesis report data.<br>Make sure you are running a local web server (e.g. <code>python -m http.server</code>)<br>and looking at the correct URL.</p>
                </div>
            `;
        }
    },

    renderDashboard: function() {
        const stats = this.data.global_stats;
        
        // Populate Top Level Metrics
        this.animateValue('stat-total', stats.total_count);
        this.animateValue('stat-adaptation', stats.ontology_breakdown.Adaptation || 0);
        this.animateValue('stat-mitigation', stats.ontology_breakdown.Mitigation || 0);
        this.animateValue('stat-water', stats.ontology_breakdown.Water || 0);

        // Populate Top Countries Preview (Sorted by count)
        const topCountries = Object.entries(this.data.country_profiles)
            .filter(([name]) => name !== 'Unknown' && name !== 'N/A')
            .sort((a,b) => b[1].count - a[1].count)
            .slice(0, 5);

        const countryContainer = document.getElementById('dashboard-top-countries');
        countryContainer.innerHTML = topCountries.map(([name, profile]) => `
            <div class="list-item-row" onclick="app.openDetail('countries', '${name.replace(/'/g, "\\'")}')">
                <span class="list-item-name">${name}</span>
                <span class="list-item-count">${profile.count} Records</span>
            </div>
        `).join('');

        // Populate Top Systems Preview
        const topSystems = Object.entries(this.data.system_profiles)
            .filter(([name]) => name !== 'general / cross-cutting' && name !== 'Unknown')
            .sort((a,b) => b[1].count - a[1].count)
            .slice(0, 5);

        const systemContainer = document.getElementById('dashboard-top-systems');
        systemContainer.innerHTML = topSystems.map(([name, profile]) => `
            <div class="list-item-row" onclick="app.openDetail('systems', '${name.replace(/'/g, "\\'")}')">
                <span class="list-item-name">${this.capitalize(name)}</span>
                <span class="list-item-count">${profile.count} Records</span>
            </div>
        `).join('');
    },

    renderMap: function() {
        const mapDiv = document.getElementById('regions_div');
        if (!mapDiv) return;

        // Clear loading spinner
        mapDiv.innerHTML = '';

        const dataArr = [['Country', 'Studies']];
        const countries = Object.entries(this.data.country_profiles)
            .filter(([name]) => name !== 'Unknown' && name !== 'N/A');
            
        for(let [name, profile] of countries) {
            dataArr.push([name, profile.count]);
        }
        
        var data = google.visualization.arrayToDataTable(dataArr);

        var options = {
            colorAxis: {colors: ['#ccfbf1', '#14b8a6', '#0f766e']}, // Light teal to dark teal
            backgroundColor: 'transparent',
            datalessRegionColor: '#f1f5f9', // Slate 100
            defaultColor: '#f8fafc',
            keepAspectRatio: true,
            tooltip: { textStyle: { color: '#0f172a', fontName: 'Inter' } },
            legend: { textStyle: { color: '#64748b', fontName: 'Inter' } }
        };

        var chart = new google.visualization.GeoChart(mapDiv);
        
        // Add click listener
        google.visualization.events.addListener(chart, 'select', () => {
            var selection = chart.getSelection();
            if (selection.length > 0) {
                var row = selection[0].row;
                var countryName = data.getValue(row, 0);
                // Open the specific country detail
                app.openDetail('countries', countryName);
            }
        });

        // Add a resize listener
        window.addEventListener('resize', () => {
            if(app.currentView === 'dashboard') {
                chart.draw(data, options);
            }
        });

        chart.draw(data, options);
    },

    renderCountriesList: function() {
        const countries = Object.entries(this.data.country_profiles)
            .sort((a,b) => b[1].count - a[1].count);
        
        document.getElementById('country-count').textContent = countries.length;
        
        const container = document.getElementById('country-list');
        container.innerHTML = countries.map(([name, profile]) => {
            return `
            <div class="list-item-row searchable-item" data-name="${name.toLowerCase()}" onclick="app.showProfileDetail('countries', '${name.replace(/'/g, "\\'")}')">
                <span class="list-item-name">${name}</span>
                <span class="list-item-count">${profile.count}</span>
            </div>
            `;
        }).join('');
    },

    renderSystemsList: function() {
        const systems = Object.entries(this.data.system_profiles)
            .sort((a,b) => b[1].count - a[1].count);
        
        document.getElementById('system-count').textContent = systems.length;
        
        const container = document.getElementById('system-list');
        container.innerHTML = systems.map(([name, profile]) => {
            return `
            <div class="list-item-row searchable-item" data-name="${name.toLowerCase()}" onclick="app.showProfileDetail('systems', '${name.replace(/'/g, "\\'")}')">
                <span class="list-item-name" style="text-transform: capitalize;">${name}</span>
                <span class="list-item-count">${profile.count}</span>
            </div>
            `;
        }).join('');
    },

    renderGapsList: function() {
        const gaps = this.data.identified_gaps;
        const container = document.getElementById('gaps-list');
        
        if(!gaps || gaps.length === 0) {
            container.innerHTML = `<div class="empty-state" style="grid-column: 1/-1"><p>No evidence gaps identified.</p></div>`;
            return;
        }

        container.innerHTML = gaps.map(gap => `
            <div class="gap-card">
                <div class="gap-header">
                    <span class="gap-area" style="text-transform: capitalize;">${gap.area}</span>
                    <span class="gap-type">${gap.type.replace('_', ' ')}</span>
                </div>
                <div class="gap-note">${gap.note}</div>
                <div class="profile-meta" style="margin-top:16px;">
                    <i class="ph ph-file-text" style="color:var(--text-muted)"></i>
                    <span style="font-size:12px; color:var(--text-muted)">Currently only ${gap.count} records retrieved</span>
                </div>
            </div>
        `).join('');
    },

    openDetail: function(view, id) {
        this.switchView(view);
        this.showProfileDetail(view, id);
    },

    showProfileDetail: function(type, id) {
        // Highlight active item in list
        const listId = type === 'countries' ? 'country-list' : 'system-list';
        document.querySelectorAll(`#${listId} .list-item-row`).forEach(row => {
            row.style.background = '';
            row.style.borderColor = 'transparent';
        });
        
        // Find the clicked row and highlight it
        const items = document.querySelectorAll(`#${listId} .searchable-item`);
        for(let item of items) {
            if(item.dataset.name === id.toLowerCase()) {
                item.style.background = 'white';
                item.style.borderColor = 'var(--secondary)';
                item.style.boxShadow = 'var(--shadow-sm)';
                break;
            }
        }

        // Render Data
        const profile = type === 'countries' 
            ? this.data.country_profiles[id] 
            : this.data.system_profiles[id];
            
        const container = document.getElementById(type === 'countries' ? 'country-detail' : 'system-detail');
        
        if(!profile) return;

        // Keep global track of citations for this profile so numbers are consistent
        // across Narrative, Adaptation, Mitigation, and Water sections.
        let urlMap = new Map();
        let urlCounter = 1;

        const parseMarkdown = (text) => {
            if (!text || text.trim() === '' || text === 'Error') return '';
            
            // 0. Remove markdown links [Text](URL) and just keep the URL
            let fixedText = text.replace(/\[([^\]]*)\]\((https?:\/\/[^\)]+)\)/g, '$2');
            
            // 0.5 Remove markdown asterisks immediately surrounding or preceding HTTP URLs to prevent parsing issues
            fixedText = fixedText.replace(/[\* \t]*(https?:\/\/[^\s\*]+)[\* \t]*/g, ' $1 ');
            // Clean up double spaces caused by the above replacement
            fixedText = fixedText.replace(/ {2,}/g, ' ');
            
            // Fix tight bullet points: add double newlines and normal markdown bullets (-)
            // so the markdown parser properly detects them as distinct list items/paragraphs.
            fixedText = fixedText.replace(/•\s*/g, '\n\n- ');
            
            // 1. Clean up "Source(s):" or "doi:" prefixes inside parentheses (or outside).
            fixedText = fixedText.replace(/\(\s*(?:Sources?|doi):\s*/gi, '(');
            fixedText = fixedText.replace(/\b(?:Sources?):\s*(https?:\/\/)/gi, '$1');
            
            // 2. Replace long URLs with superscript numbers that act as links
            fixedText = fixedText.replace(/(?:[A-Za-z]+:\s*)?(https?:\/\/[^\s\)]+)/g, (match, url) => {
                let trailing = '';
                while (url.endsWith(',') || url.endsWith('.')) {
                    trailing = url.slice(-1) + trailing;
                    url = url.slice(0, -1);
                }
                
                if (!urlMap.has(url)) {
                    urlMap.set(url, urlCounter++);
                }
                const citationNum = urlMap.get(url);
                return `<sup>[<a href="${url}" target="_blank" title="${url}">${citationNum}</a>]</sup>${trailing}`;
            });
            
            // 3. Remove enclosing parentheses if they ONLY contain our superscripts (and spaces/commas/etc)
            fixedText = fixedText.replace(/\(\s*(?:<sup>.*?<\/sup>[\s,]*)+\s*\)/g, (match) => {
                return match.replace(/^\(\s*/, '').replace(/\s*\)$/, '');
            });
            
            return typeof marked !== 'undefined' ? marked.parse(fixedText) : fixedText;
        };

        // Parse markdown narrative
        const formattedNarrative = parseMarkdown(profile.narrative || 'No synthesis narrative available.');

        const formattedAdaptation = parseMarkdown(profile.adaptation);
        const formattedMitigation = parseMarkdown(profile.mitigation);
        const formattedWater = parseMarkdown(profile.water);

        const ontologiesHtml = `
            ${formattedAdaptation ? `
            <div class="collapsible-section" style="border: 1px solid #bbf7d0;">
                <div class="collapsible-header" style="background-color: #f0fdf4;" onclick="this.parentElement.classList.toggle('expanded')">
                    <h3 style="font-size: 15px; margin: 0; color: #166534; font-weight: 600;"><i class="ph ph-leaf" style="margin-right: 6px; position: relative; top: 2px;"></i>Adaptation</h3>
                    <i class="ph ph-caret-down" style="color: #166534;"></i>
                </div>
                <div class="collapsible-content" style="background-color: #f0fdf4;">
                    <div style="font-size: 14px; color: #14532d; line-height: 1.5; padding-bottom: 20px;">${formattedAdaptation}</div>
                </div>
            </div>` : ''}
            
            ${formattedMitigation ? `
            <div class="collapsible-section" style="border: 1px solid #bfdbfe; margin-top: 16px;">
                <div class="collapsible-header" style="background-color: #eff6ff;" onclick="this.parentElement.classList.toggle('expanded')">
                    <h3 style="font-size: 15px; margin: 0; color: #1e40af; font-weight: 600;"><i class="ph ph-wind" style="margin-right: 6px; position: relative; top: 2px;"></i>Mitigation</h3>
                    <i class="ph ph-caret-down" style="color: #1e40af;"></i>
                </div>
                <div class="collapsible-content" style="background-color: #eff6ff;">
                    <div style="font-size: 14px; color: #1e3a8a; line-height: 1.5; padding-bottom: 20px;">${formattedMitigation}</div>
                </div>
            </div>` : ''}
            
            ${formattedWater ? `
            <div class="collapsible-section" style="border: 1px solid #bae6fd; margin-top: 16px;">
                <div class="collapsible-header" style="background-color: #f0f9ff;" onclick="this.parentElement.classList.toggle('expanded')">
                    <h3 style="font-size: 15px; margin: 0; color: #0369a1; font-weight: 600;"><i class="ph ph-drop" style="margin-right: 6px; position: relative; top: 2px;"></i>Water</h3>
                    <i class="ph ph-caret-down" style="color: #0369a1;"></i>
                </div>
                <div class="collapsible-content" style="background-color: #f0f9ff;">
                    <div style="font-size: 14px; color: #0c4a6e; line-height: 1.5; padding-bottom: 20px;">${formattedWater}</div>
                </div>
            </div>` : ''}
        `;

        // Build DOIs
        const doisHtml = (profile.top_dois || []).slice(0, 10).map(doiObj => {
            const isDict = typeof doiObj === 'object' && doiObj !== null;
            const doiString = isDict ? (doiObj.doi || '') : (doiObj || '');
            const cleanDoi = doiString.replace('doi:', '');
            
            if (isDict) {
                const title = doiObj.title || '';
                const citations = doiObj.citations || 0;
                const downloads = doiObj.downloads || 0;
                const views = doiObj.views || 0;
                const repository = doiObj.repository || '';
                
                let sourceClass = '';
                if (repository.toLowerCase().includes('cgspace')) sourceClass = 'source-cgspace';
                else if (repository.toLowerCase().includes('dataverse')) sourceClass = 'source-dataverse';
                
                return `
                    <a href="https://doi.org/${cleanDoi}" target="_blank" class="source-card ${sourceClass}">
                        <div class="source-header">
                            <i class="ph ph-article"></i>
                            <span class="source-title" title="${title}">${title || cleanDoi}</span>
                        </div>
                        <div class="source-metrics">
                            <div class="metric citations" title="Citations">
                                <img src="src/icons/citation.png" alt="Citations" style="width: 18px; height: 18px; opacity: 0.8; object-fit: contain;">
                                <div class="metric-info">
                                    <span class="metric-value">${citations.toLocaleString()}</span>
                                    <span class="metric-label">Cites</span>
                                </div>
                            </div>
                            <div class="metric downloads" title="Downloads">
                                <i class="ph ph-download-simple"></i>
                                <div class="metric-info">
                                    <span class="metric-value">${downloads.toLocaleString()}</span>
                                    <span class="metric-label">Downs</span>
                                </div>
                            </div>
                            <div class="metric views" title="Views">
                                <i class="ph ph-eye"></i>
                                <div class="metric-info">
                                    <span class="metric-value">${views.toLocaleString()}</span>
                                    <span class="metric-label">Views</span>
                                </div>
                            </div>
                        </div>
                        <div class="source-footer">
                            <i class="ph ph-link"></i>
                            <span>${cleanDoi}</span>
                        </div>
                    </a>
                `;
            } else {
                return `
                    <a href="https://doi.org/${cleanDoi}" target="_blank" class="doi-card">
                        <i class="ph ph-link"></i>
                        <span style="font-size: 13px; font-family: monospace; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${cleanDoi}</span>
                    </a>
                `;
            }
        }).join('');

        container.innerHTML = `
            <div class="profile-title-area animation-fadeIn">
                <h2 style="font-size: 32px; text-transform: ${type === 'systems' ? 'capitalize' : 'none'};">${id}</h2>
                <div class="profile-meta">
                    <span class="badge">${profile.count} Research Records Analyzed only the top ranked research are included</span>
                </div>
            </div>
            
            <div class="synthesized-narrative animation-fadeIn" style="animation-delay: 0.1s;">
                ${formattedNarrative}
            </div>

            <div class="ontologies-container animation-fadeIn" style="animation-delay: 0.15s; margin-top: 20px; margin-bottom: 24px;">
                ${ontologiesHtml}
            </div>

            ${profile.top_dois && profile.top_dois.length > 0 ? `
            <div class="collapsible-section animation-fadeIn" style="animation-delay: 0.2s;">
                <div class="collapsible-header" onclick="this.parentElement.classList.toggle('expanded')">
                    <div>
                        <h3 style="font-size: 16px; margin: 0;">Top Source Evidence</h3>
                        <p style="font-size: 13px; color: var(--text-muted); margin: 4px 0 0 0;">External links open in a new tab to doi.org resolution service.</p>
                    </div>
                    <i class="ph ph-caret-down"></i>
                </div>
                <div class="collapsible-content">
                    <div class="doi-list" style="margin-top: 0;">
                        ${doisHtml}
                    </div>
                </div>
            </div>
            ` : ''}
        `;
        
        // Reset scroll position
        container.scrollTop = 0;
    },

    handleSearch: function(term, targetView) {
        if(targetView !== 'countries' && targetView !== 'systems') return;
        
        const listId = targetView === 'countries' ? 'country-list' : 'system-list';
        const items = document.querySelectorAll(`#${listId} .searchable-item`);
        
        items.forEach(item => {
            if(term === '' || item.dataset.name.includes(term)) {
                item.style.display = 'flex';
            } else {
                item.style.display = 'none';
            }
        });
    },

    // Utility: Animate numbers counting up
    animateValue: function(id, end, duration = 1000) {
        const obj = document.getElementById(id);
        if(!obj) return;
        
        let startTimestamp = null;
        const step = (timestamp) => {
            if (!startTimestamp) startTimestamp = timestamp;
            const progress = Math.min((timestamp - startTimestamp) / duration, 1);
            // Ease out quad
            const easeProgress = progress * (2 - progress);
            obj.innerHTML = Math.floor(easeProgress * end).toLocaleString();
            if (progress < 1) {
                window.requestAnimationFrame(step);
            } else {
                obj.innerHTML = end.toLocaleString();
            }
        };
        window.requestAnimationFrame(step);
    },

    capitalize: function(str) {
        return str.replace(/\b\w/g, l => l.toUpperCase());
    }
};

// Initialize after DOM load
document.addEventListener('DOMContentLoaded', () => {
    app.init();
});
