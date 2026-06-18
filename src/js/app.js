const app = {
    data: null,
    currentView: 'dashboard',
    activeTopic: null,

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

        // Topic card clicks
        document.querySelectorAll('.metric-card.clickable-card').forEach(card => {
            card.addEventListener('click', (e) => {
                const topic = e.currentTarget.dataset.topic;
                
                // Toggle topic
                if (topic === 'all') {
                    this.activeTopic = null;
                } else if (this.activeTopic === topic) {
                    this.activeTopic = null;
                } else {
                    this.activeTopic = topic;
                }
                
                // Update UI visually
                document.querySelectorAll('.metric-card.clickable-card').forEach(c => c.classList.remove('active-card'));
                if (this.activeTopic) {
                    e.currentTarget.classList.add('active-card');
                } else {
                    // Activate the Total Records card
                    const totalCard = document.getElementById('total-records-card');
                    if (totalCard) totalCard.classList.add('active-card');
                }

                // Re-render dashboard components that depend on data
                this.renderDashboard();
                this.renderMap();
                this.renderCountriesList();
                this.renderSystemsList();
            });
        });
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

        if (viewId === 'datasets') this.renderDatasetsView();

        // Update titles
        const titles = {
            'dashboard': 'Global Overview',
            'countries': 'Country Synthesis',
            'systems':   'Production Systems',
            'datasets':  'Open Datasets',
            'gaps':      'Evidence Gaps',
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
        // We only modify the top lists dynamically. The top header global stats stay constant,
        // EXCEPT we could modify them, but usually they represent total overall stats. 
        // We'll leave the 4 global metric cards untouched here because they act as toggles themselves.
        if (this.data && !this.activeTopic && document.getElementById('stat-total').innerHTML === '0') {
            const stats = this.data.global_stats;
            this.animateValue('stat-total', stats.total_count);
            this.animateValue('stat-adaptation', stats.ontology_breakdown.Adaptation || 0);
            this.animateValue('stat-mitigation', stats.ontology_breakdown.Mitigation || 0);
            this.animateValue('stat-water', stats.ontology_breakdown.Water || 0);
        }

        const getDisplayCount = (profile) => {
            if (!this.activeTopic) return profile.count;
            return (profile.ontology_breakdown && profile.ontology_breakdown[this.activeTopic]) || 0;
        };

        // Populate Top Countries Preview (Sorted by display count)
        const topCountries = Object.entries(this.data.country_profiles)
            .filter(([name]) => name !== 'Unknown' && name !== 'N/A')
            .map(([name, profile]) => ({ name, profile, displayCount: getDisplayCount(profile) }))
            .filter(item => item.displayCount > 0)
            .sort((a,b) => b.displayCount - a.displayCount)
            .slice(0, 5);

        const countryContainer = document.getElementById('dashboard-top-countries');
        countryContainer.innerHTML = topCountries.map(({name, displayCount}) => `
            <div class="list-item-row" onclick="app.openDetail('countries', '${name.replace(/'/g, "\\'")}')">
                <span class="list-item-name">${name}</span>
                <span class="list-item-count">${displayCount} Records</span>
            </div>
        `).join('');

        // Populate Top Systems Preview
        const topSystems = Object.entries(this.data.system_profiles)
            .filter(([name]) => name !== 'general / cross-cutting' && name !== 'Unknown')
            .map(([name, profile]) => ({ name, profile, displayCount: getDisplayCount(profile) }))
            .filter(item => item.displayCount > 0)
            .sort((a,b) => b.displayCount - a.displayCount)
            .slice(0, 5);

        const systemContainer = document.getElementById('dashboard-top-systems');
        systemContainer.innerHTML = topSystems.map(({name, displayCount}) => `
            <div class="list-item-row" onclick="app.openDetail('systems', '${name.replace(/'/g, "\\'")}')">
                <span class="list-item-name">${this.capitalize(name)}</span>
                <span class="list-item-count">${displayCount} Records</span>
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
            let count = profile.count;
            if (this.activeTopic) {
                count = (profile.ontology_breakdown && profile.ontology_breakdown[this.activeTopic]) || 0;
            }
            if (count > 0) {
                dataArr.push([name, count]);
            }
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
        const getDisplayCount = (profile) => {
            if (!this.activeTopic) return profile.count;
            return (profile.ontology_breakdown && profile.ontology_breakdown[this.activeTopic]) || 0;
        };

        const countries = Object.entries(this.data.country_profiles)
            .filter(([name]) => name !== 'Unknown' && name !== 'N/A' && name !== '')
            .map(([name, profile]) => ({ name, profile, displayCount: getDisplayCount(profile) }))
            .filter(item => item.displayCount > 0)
            .sort((a,b) => b.displayCount - a.displayCount);
        
        document.getElementById('country-count').textContent = countries.length;
        
        const container = document.getElementById('country-list');
        container.innerHTML = countries.map(({name, displayCount}) => {
            return `
            <div class="list-item-row searchable-item" data-name="${name.toLowerCase()}" onclick="app.showProfileDetail('countries', '${name.replace(/'/g, "\\'")}')">
                <span class="list-item-name">${name}</span>
                <span class="list-item-count">${displayCount}</span>
            </div>
            `;
        }).join('');
    },

    renderSystemsList: function() {
        const getDisplayCount = (profile) => {
            if (!this.activeTopic) return profile.count;
            return (profile.ontology_breakdown && profile.ontology_breakdown[this.activeTopic]) || 0;
        };

        const systems = Object.entries(this.data.system_profiles)
            .map(([name, profile]) => ({ name, profile, displayCount: getDisplayCount(profile) }))
            .filter(item => item.displayCount > 0)
            .sort((a,b) => b.displayCount - a.displayCount);
        
        document.getElementById('system-count').textContent = systems.length;
        
        const container = document.getElementById('system-list');
        container.innerHTML = systems.map(({name, displayCount}) => {
            return `
            <div class="list-item-row searchable-item" data-name="${name.toLowerCase()}" onclick="app.showProfileDetail('systems', '${name.replace(/'/g, "\\'")}')">
                <span class="list-item-name" style="text-transform: capitalize;">${name}</span>
                <span class="list-item-count">${displayCount}</span>
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

        // Build Datasets
        const datasetTypeMeta = {
            spatial:       { icon: 'ph-map-trifold',   label: 'Spatial',       cls: 'dtype-spatial' },
            tabular:       { icon: 'ph-table',          label: 'Tabular',       cls: 'dtype-tabular' },
            unstructured:  { icon: 'ph-file-text',      label: 'Unstructured',  cls: 'dtype-unstructured' },
            unknown:       { icon: 'ph-question',       label: 'Unknown',       cls: 'dtype-unknown' },
        };
        const datasetsHtml = (profile.top_datasets || []).slice(0, 10).map(doiObj => {
            const isDict = typeof doiObj === 'object' && doiObj !== null;
            const doiString = isDict ? (doiObj.doi || '') : (doiObj || '');
            const cleanDoi = doiString.replace('doi:', '');

            if (isDict) {
                const title = doiObj.title || '';
                const citations = doiObj.citations || 0;
                const downloads = doiObj.downloads || 0;
                const views = doiObj.views || 0;
                const repository = doiObj.repository || '';
                const dtype = (doiObj.dataset_type || 'unknown').toLowerCase();
                const dtMeta = datasetTypeMeta[dtype] || datasetTypeMeta.unknown;

                let sourceClass = '';
                if (repository.toLowerCase().includes('cgspace')) sourceClass = 'source-cgspace';
                else if (repository.toLowerCase().includes('dataverse')) sourceClass = 'source-dataverse';

                return `
                    <a href="https://doi.org/${cleanDoi}" target="_blank" class="source-card ${sourceClass}">
                        <div class="source-header">
                            <i class="ph ph-database"></i>
                            <span class="source-title" title="${title}">${title || cleanDoi}</span>
                            <span class="dataset-type-badge ${dtMeta.cls}" title="Dataset type">
                                <i class="ph ${dtMeta.icon}"></i>${dtMeta.label}
                            </span>
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
                    </a>
                `;
            } else {
                return `
                    <a href="https://doi.org/${cleanDoi}" target="_blank" class="doi-card">
                        <i class="ph ph-database"></i>
                        <span style="font-size: 13px; font-family: monospace; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">${cleanDoi}</span>
                    </a>
                `;
            }
        }).join('');

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

        const datasetCount = profile.dataset_count || 0;

        container.innerHTML = `
            <div class="profile-title-area animation-fadeIn">
                <h2 style="text-transform: ${type === 'systems' ? 'capitalize' : 'none'};">${id}</h2>
                <div class="profile-meta" style="gap: 8px; flex-wrap: wrap;">
                    <span class="badge"><i class="ph ph-article" style="margin-right:4px; position:relative; top:2px;"></i>${profile.count} Records</span>
                    <span class="badge"><i class="ph ph-database" style="margin-right:4px; position:relative; top:2px;"></i>${datasetCount} Dataset${datasetCount !== 1 ? 's' : ''}</span>
                </div>
            </div>

            <div class="detail-tabs animation-fadeIn" style="animation-delay: 0.05s;">
                <button class="tab-btn active" onclick="app.switchDetailTab(this, 'research')">
                    <i class="ph ph-article"></i> Research
                    <span class="tab-count">${profile.count}</span>
                </button>
                <button class="tab-btn" onclick="app.switchDetailTab(this, 'datasets', '${type}', '${id.replace(/'/g,"\\'")}')">
                    <i class="ph ph-database"></i> Datasets
                    <span class="tab-count" id="profile-ds-count">${datasetCount}</span>
                </button>
            </div>

            <div class="tab-panel" data-tab="research">
                <div class="synthesized-narrative animation-fadeIn">
                    ${formattedNarrative}
                </div>

                <div class="ontologies-container animation-fadeIn" style="margin-top: 20px; margin-bottom: 24px;">
                    ${ontologiesHtml}
                </div>

                ${profile.top_dois && profile.top_dois.length > 0 ? `
                <div class="collapsible-section animation-fadeIn">
                    <div class="collapsible-header" onclick="this.parentElement.classList.toggle('expanded')">
                        <div>
                            <h3 style="font-size: 16px; margin: 0;">Top Source Evidence</h3>
                            <p style="font-size: 13px; color: var(--text-muted); margin: 4px 0 0 0;">External links open in a new tab to doi.org resolution service.</p>
                        </div>
                        <i class="ph ph-caret-down"></i>
                    </div>
                    <div class="collapsible-content">
                        <div class="doi-list" style="margin-top: 0;">${doisHtml}</div>
                    </div>
                </div>` : ''}
            </div>

            <div class="tab-panel hidden" data-tab="datasets" id="profile-datasets-panel"
                 data-group-type="${type}" data-group-name="${id.replace(/"/g,'&quot;')}"
                 data-type-filter="all" data-topic-filter="all">
                <div class="dataset-empty-state">
                    <i class="ph ph-spinner ph-spin"></i>
                    <p>Click to load datasets</p>
                </div>
            </div>
        `;

        // Reset scroll position
        container.scrollTop = 0;
    },

    switchDetailTab: function(btn, tabId, groupType, groupName) {
        const panel = btn.closest('.detail-panel');
        panel.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        panel.querySelectorAll('.tab-panel').forEach(p => p.classList.add('hidden'));
        const tabPanel = panel.querySelector(`.tab-panel[data-tab="${tabId}"]`);
        tabPanel.classList.remove('hidden');
        if (tabId === 'datasets' && groupType && groupName) {
            this.renderProfileDatasets(tabPanel, groupType, groupName);
        }
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
    },

    // ── Datasets View ────────────────────────────────────────────────────────

    _dsState: { ranking: 'most_downloaded', typeFilter: 'all', topicFilter: 'all' },
    _dsData: null,   // cached array from datasets.json

    _dsRankKey: { most_downloaded: 'downloads_count', most_cited: 'citation_count', most_viewed: 'total_views' },

    renderDatasetsView: function() {
        const root = document.getElementById('datasets-view-root');
        if (!root) return;

        if (!this._dsData) {
            root.innerHTML = `<div style="padding:60px;text-align:center;color:var(--text-muted)">
                <i class="ph ph-spinner ph-spin" style="font-size:28px"></i>
                <p style="margin-top:14px;font-size:14px">Loading datasets...</p></div>`;
            fetch('cgiar_mas_agent2/output/datasets.json')
                .then(r => { if (!r.ok) throw new Error('datasets.json not found'); return r.json(); })
                .then(data => {
                    // Normalise fields so filters work consistently
                    this._dsData = data.map(d => ({
                        ...d,
                        dataset_type:    ((d.dataset_type||'').trim().toLowerCase() || 'unknown'),
                        ontology_tags:   Array.isArray(d.ontology_tags) ? d.ontology_tags
                                         : (d.ontology_tags||'').split(',').map(t=>t.trim()).filter(Boolean),
                        citation_count:  +(d.citation_count||0),
                        downloads_count: +(d.downloads_count||0),
                        total_views:     +(d.total_views||0),
                    }));
                    this.renderDatasetsView();
                })
                .catch(err => {
                    root.innerHTML = `<div class="dataset-empty-state">
                        <i class="ph ph-warning-circle" style="color:#ef4444"></i>
                        <p>Could not load datasets.json.<br>Run <code>python build_datasets_json.py</code> first.</p></div>`;
                });
            return;
        }

        const all = this._dsData;

        // Stats from full dataset
        const typeCounts = { spatial: 0, tabular: 0, unstructured: 0, unknown: 0 };
        all.forEach(d => { const t = d.dataset_type || 'unknown'; typeCounts[t] !== undefined ? typeCounts[t]++ : typeCounts.unknown++; });

        const countInter = (t1, t2) => all.filter(d => (d.ontology_tags||[]).includes(t1) && (d.ontology_tags||[]).includes(t2)).length;
        const countMulti = () => all.filter(d => (d.ontology_tags||[]).length > 1).length;

        const dtMeta = {
            spatial:      { icon: 'ph-map-trifold', label: 'Spatial',      cls: 'dtype-spatial',      color: '#1B6B47', bg: '#E6F4EA' },
            tabular:      { icon: 'ph-table',        label: 'Tabular',      cls: 'dtype-tabular',      color: '#2B5BB5', bg: '#E8F0FD' },
            unstructured: { icon: 'ph-file-text',    label: 'Unstructured', cls: 'dtype-unstructured', color: '#7A5C3A', bg: '#F5F0E8' },
            unknown:      { icon: 'ph-question',     label: 'Unknown',      cls: 'dtype-unknown',      color: '#9CA3AF', bg: '#F3F4F6' },
        };

        root.innerHTML = `
            <div class="ds-stat-grid">
                <div class="ds-stat-card">
                    <div class="ds-stat-icon" style="background:#F0F7F4"><i class="ph ph-database" style="color:#0B3D2E"></i></div>
                    <div><div class="ds-stat-value">${all.length.toLocaleString()}</div><div class="ds-stat-label">Total Datasets</div></div>
                </div>
                ${['spatial','tabular','unstructured'].map(t => `
                <div class="ds-stat-card">
                    <div class="ds-stat-icon" style="background:${dtMeta[t].bg}">
                        <i class="ph ${dtMeta[t].icon}" style="color:${dtMeta[t].color}"></i>
                    </div>
                    <div><div class="ds-stat-value">${typeCounts[t].toLocaleString()}</div><div class="ds-stat-label">${dtMeta[t].label}</div></div>
                </div>`).join('')}
            </div>

            <div class="glass-panel ds-filters-panel">
                <div class="ds-filter-row">
                    <span class="ds-filter-label">Type</span>
                    <div class="ds-filter-group">
                        ${['all','spatial','tabular','unstructured','unknown'].map(t => {
                            const n = t === 'all' ? all.length : all.filter(d => d.dataset_type === t).length;
                            if (t !== 'all' && n === 0) return '';
                            return `<button class="ds-filter-btn ${this._dsState.typeFilter===t?'active':''} ${t!=='all'?'dtype-btn-'+t:''}"
                                onclick="app.setDsFilter('type','${t}')">
                                ${t!=='all'&&dtMeta[t]?`<i class="ph ${dtMeta[t].icon}"></i>`:''}
                                ${t==='all'?'All':this.capitalize(t)}
                                <span class="ds-btn-count">${n.toLocaleString()}</span>
                            </button>`;
                        }).join('')}
                    </div>
                </div>
                <div class="ds-filter-row">
                    <span class="ds-filter-label">Topic</span>
                    <div class="ds-filter-group">
                        ${[['all','All Topics'],['Adaptation','Adaptation'],['Mitigation','Mitigation'],['Water','Water'],['multi','Multi-topic']].map(([t,label]) => {
                            const n = t==='all' ? all.length : t==='multi' ? all.filter(d=>(d.ontology_tags||[]).length>1).length : all.filter(d=>(d.ontology_tags||[]).includes(t)).length;
                            if (t !== 'all' && n === 0) return '';
                            return `<button class="ds-filter-btn ${this._dsState.topicFilter===t?'active':''} ${t!=='all'?'topic-btn-'+t.toLowerCase():''}"
                                onclick="app.setDsFilter('topic','${t}')">
                                ${t==='multi'?'<i class="ph ph-intersect"></i> ':t!=='all'?`<span class="topic-dot topic-dot-${t.toLowerCase()}"></span>`:''}${label}
                                <span class="ds-btn-count">${n.toLocaleString()}</span>
                            </button>`;
                        }).join('')}
                    </div>
                </div>
                <div class="ds-intersections">
                    <span class="ds-inter-label">Cross-cutting:</span>
                    ${[['Adaptation','Water','adapt-water'],['Adaptation','Mitigation','adapt-mitig'],['Mitigation','Water','mitig-water']]
                        .map(([t1,t2,cls]) => { const n = countInter(t1,t2); return n > 0 ? `
                        <span class="ds-inter-pill ds-inter-${cls}">
                            <span class="topic-dot topic-dot-${t1.toLowerCase()}"></span>${t1}
                            <span style="opacity:.5;margin:0 2px">+</span>
                            <span class="topic-dot topic-dot-${t2.toLowerCase()}"></span>${t2}
                            <strong>${n}</strong>
                        </span>` : ''; }).join('')}
                    ${countMulti()>0?`<span class="ds-inter-pill" style="background:var(--background);border:1px solid var(--border)">
                        <i class="ph ph-intersect" style="font-size:11px"></i> Multi-topic <strong>${countMulti()}</strong></span>`:''}
                </div>
            </div>

            <div class="glass-panel" style="padding:0;overflow:hidden">
                <div class="ds-ranking-tabs">
                    ${[['most_downloaded','ph-download-simple','Most Downloaded'],['most_cited','ph-quotes','Most Cited'],['most_viewed','ph-eye','Most Viewed']].map(([key,icon,label]) => `
                    <button class="ds-rank-tab ${this._dsState.ranking===key?'active':''}" onclick="app.setDsFilter('ranking','${key}')">
                        <i class="ph ${icon}"></i>${label}
                    </button>`).join('')}
                </div>
                <div id="ds-cards-container" style="padding:20px">${this._renderDsCards()}</div>
            </div>`;
    },

    _renderDsCards: function() {
        if (!this._dsData) return '';

        const sortKey = this._dsRankKey[this._dsState.ranking] || 'downloads_count';
        const topicColor = { Adaptation: '#1B6B47', Mitigation: '#2B5BB5', Water: '#1A5276' };
        const dtMeta = {
            spatial:      { icon: 'ph-map-trifold', cls: 'dtype-spatial',      label: 'Spatial' },
            tabular:      { icon: 'ph-table',        cls: 'dtype-tabular',      label: 'Tabular' },
            unstructured: { icon: 'ph-file-text',    cls: 'dtype-unstructured', label: 'Unstructured' },
            unknown:      { icon: 'ph-question',     cls: 'dtype-unknown',      label: 'Unknown' },
        };

        const filtered = this._dsData
            .filter(d => {
                const typeOk = this._dsState.typeFilter === 'all' || (d.dataset_type||'unknown') === this._dsState.typeFilter;
                const tags   = d.ontology_tags || [];
                const topicOk = this._dsState.topicFilter === 'all'   ? true
                              : this._dsState.topicFilter === 'multi'  ? tags.length > 1
                              : tags.includes(this._dsState.topicFilter);
                return typeOk && topicOk;
            })
            .sort((a, b) => (b[sortKey] || 0) - (a[sortKey] || 0))
            .slice(0, 150);

        if (!filtered.length) return `<div class="dataset-empty-state">
            <i class="ph ph-funnel-x"></i><p>No datasets match the selected filters.</p></div>`;

        return `<p style="font-size:12px;color:var(--text-muted);margin-bottom:14px">
            Showing top ${filtered.length} of ${this._dsData.filter(d => {
                const typeOk = this._dsState.typeFilter==='all'||(d.dataset_type||'unknown')===this._dsState.typeFilter;
                const tags=d.ontology_tags||[];
                const topicOk=this._dsState.topicFilter==='all'?true:this._dsState.topicFilter==='multi'?tags.length>1:tags.includes(this._dsState.topicFilter);
                return typeOk&&topicOk;
            }).length.toLocaleString()} matching datasets, sorted by ${this._dsState.ranking.replace('_',' ').replace('most ','')}</p>
        <div class="doi-list" style="margin:0">${filtered.map(d => {
            const cleanDoi = (d.doi_pid||'').replace('doi:','');
            const dtype = (d.dataset_type||'unknown').toLowerCase();
            const dm = dtMeta[dtype] || dtMeta.unknown;
            const repo = d.repository_source || '';
            const srcCls = repo.toLowerCase().includes('cgspace') ? 'source-cgspace'
                         : repo.toLowerCase().includes('dataverse') ? 'source-dataverse' : '';
            const ontoBadges = (d.ontology_tags||[]).map(t =>
                `<span class="ds-onto-badge" style="background:${topicColor[t]||'#6B7280'}20;color:${topicColor[t]||'#6B7280'};border-color:${topicColor[t]||'#6B7280'}40">
                    <span class="topic-dot topic-dot-${t.toLowerCase()}"></span>${t}</span>`).join('');

            return `<a href="https://doi.org/${cleanDoi}" target="_blank" class="source-card ${srcCls}">
                <div class="source-header">
                    <i class="ph ph-database"></i>
                    <span class="source-title" title="${d.title}">${d.title||cleanDoi}</span>
                    <span class="dataset-type-badge ${dm.cls}"><i class="ph ${dm.icon}"></i>${dm.label}</span>
                </div>
                ${ontoBadges?`<div class="ds-onto-row">${ontoBadges}</div>`:''}
                <div class="source-metrics">
                    <div class="metric citations">
                        <img src="src/icons/citation.png" alt="Citations" style="width:18px;height:18px;opacity:.8;object-fit:contain;">
                        <div class="metric-info">
                            <span class="metric-value">${(+(d.citation_count||0)).toLocaleString()}</span>
                            <span class="metric-label">Cites</span>
                        </div>
                    </div>
                    <div class="metric downloads">
                        <i class="ph ph-download-simple"></i>
                        <div class="metric-info">
                            <span class="metric-value">${(+(d.downloads_count||0)).toLocaleString()}</span>
                            <span class="metric-label">Downs</span>
                        </div>
                    </div>
                    <div class="metric views">
                        <i class="ph ph-eye"></i>
                        <div class="metric-info">
                            <span class="metric-value">${(+(d.total_views||0)).toLocaleString()}</span>
                            <span class="metric-label">Views</span>
                        </div>
                    </div>
                </div>
            </a>`;
        }).join('')}</div>`;
    },

    setDsFilter: function(kind, value) {
        if (kind === 'ranking') this._dsState.ranking     = value;
        if (kind === 'type')    this._dsState.typeFilter   = value;
        if (kind === 'topic')   this._dsState.topicFilter  = value;
        this.renderDatasetsView();
    },

    renderProfileDatasets: function(panelEl, groupType, groupName, typeFilter, topicFilter) {
        if (!panelEl) return;
        typeFilter  = typeFilter  || panelEl.dataset.typeFilter  || 'all';
        topicFilter = topicFilter || panelEl.dataset.topicFilter || 'all';
        panelEl.dataset.typeFilter  = typeFilter;
        panelEl.dataset.topicFilter = topicFilter;

        if (!this._dsData) {
            panelEl.innerHTML = `<div style="padding:40px;text-align:center;color:var(--text-muted)">
                <i class="ph ph-spinner ph-spin" style="font-size:24px"></i>
                <p style="margin-top:12px;font-size:13px">Loading datasets...</p></div>`;
            fetch('cgiar_mas_agent2/output/datasets.json')
                .then(r => r.json())
                .then(data => {
                    this._dsData = data.map(d => ({
                        ...d,
                        dataset_type:    ((d.dataset_type||'').trim().toLowerCase() || 'unknown'),
                        ontology_tags:   Array.isArray(d.ontology_tags) ? d.ontology_tags : (d.ontology_tags||'').split(',').map(t=>t.trim()).filter(Boolean),
                        citation_count:  +(d.citation_count||0),
                        downloads_count: +(d.downloads_count||0),
                        total_views:     +(d.total_views||0),
                    }));
                    this.renderProfileDatasets(panelEl, groupType, groupName, typeFilter, topicFilter);
                })
                .catch(() => { panelEl.innerHTML = `<div class="dataset-empty-state"><i class="ph ph-warning-circle"></i><p>Could not load datasets.json.</p></div>`; });
            return;
        }

        const colKey = groupType === 'systems' ? 'production_system' : 'country';
        const nameLower = groupName.toLowerCase();

        // Match records belonging to this group (multi-valued cells are comma-separated)
        const groupData = this._dsData.filter(d => {
            const val = (d[colKey] || '').toLowerCase();
            return val.split(',').map(v => v.trim()).includes(nameLower);
        });

        const dtMeta = {
            spatial:      { icon: 'ph-map-trifold', label: 'Spatial',      cls: 'dtype-spatial',      color: '#1B6B47', bg: '#E6F4EA' },
            tabular:      { icon: 'ph-table',        label: 'Tabular',      cls: 'dtype-tabular',      color: '#2B5BB5', bg: '#E8F0FD' },
            unstructured: { icon: 'ph-file-text',    label: 'Unstructured', cls: 'dtype-unstructured', color: '#7A5C3A', bg: '#F5F0E8' },
            unknown:      { icon: 'ph-question',     label: 'Unknown',      cls: 'dtype-unknown',      color: '#9CA3AF', bg: '#F3F4F6' },
        };
        const topicColor = { Adaptation: '#1B6B47', Mitigation: '#2B5BB5', Water: '#1A5276' };

        const typeCount  = t => groupData.filter(d => d.dataset_type === t).length;
        const topicCount = t => t === 'multi' ? groupData.filter(d => (d.ontology_tags||[]).length > 1).length
                                              : groupData.filter(d => (d.ontology_tags||[]).includes(t)).length;

        // Apply filters
        const filtered = groupData.filter(d => {
            const typeOk  = typeFilter === 'all'  || d.dataset_type === typeFilter;
            const tags    = d.ontology_tags || [];
            const topicOk = topicFilter === 'all'  ? true
                          : topicFilter === 'multi' ? tags.length > 1
                          : tags.includes(topicFilter);
            return typeOk && topicOk;
        }).sort((a, b) => (b.downloads_count||0) - (a.downloads_count||0));

        // Update tab count badge
        const countBadge = document.getElementById('profile-ds-count');
        if (countBadge) countBadge.textContent = groupData.length;

        const safeGroup = groupName.replace(/'/g, "\\'").replace(/"/g, '&quot;');

        const typeFilters = ['all','spatial','tabular','unstructured','unknown'].map(t => {
            const n = t === 'all' ? groupData.length : typeCount(t);
            if (t !== 'all' && n === 0) return '';
            return `<button class="ds-filter-btn ${typeFilter===t?'active':''} ${t!=='all'?'dtype-btn-'+t:''}"
                onclick="app.renderProfileDatasets(document.getElementById('profile-datasets-panel'),'${groupType}','${safeGroup}','${t}','${topicFilter}')">
                ${t!=='all'&&dtMeta[t]?`<i class="ph ${dtMeta[t].icon}"></i>`:''}${t==='all'?'All':this.capitalize(t)}
                <span class="ds-btn-count">${n}</span>
            </button>`;
        }).join('');

        const topicFilters = [['all','All'],['Adaptation','Adaptation'],['Mitigation','Mitigation'],['Water','Water'],['multi','Multi-topic']].map(([t, label]) => {
            const n = t === 'all' ? groupData.length : topicCount(t);
            if (t !== 'all' && n === 0) return '';
            return `<button class="ds-filter-btn ${topicFilter===t?'active':''} ${t!=='all'?'topic-btn-'+t.toLowerCase():''}"
                onclick="app.renderProfileDatasets(document.getElementById('profile-datasets-panel'),'${groupType}','${safeGroup}','${typeFilter}','${t}')">
                ${t==='multi'?'<i class="ph ph-intersect"></i> ':t!=='all'?`<span class="topic-dot topic-dot-${t.toLowerCase()}"></span>`:''}${label}
                <span class="ds-btn-count">${n}</span>
            </button>`;
        }).join('');

        const cards = filtered.length === 0
            ? `<div class="dataset-empty-state"><i class="ph ph-funnel-x"></i><p>No datasets match the selected filters.</p></div>`
            : `<div class="doi-list" style="margin:0">${filtered.map(d => {
                const cleanDoi = (d.doi_pid||'').replace('doi:','');
                const dtype = d.dataset_type || 'unknown';
                const dm = dtMeta[dtype] || dtMeta.unknown;
                const srcCls = (d.repository_source||'').toLowerCase().includes('dataverse') ? 'source-dataverse'
                             : (d.repository_source||'').toLowerCase().includes('cgspace')   ? 'source-cgspace' : '';
                const ontoBadges = (d.ontology_tags||[]).map(t =>
                    `<span class="ds-onto-badge" style="background:${topicColor[t]||'#6B7280'}20;color:${topicColor[t]||'#6B7280'};border-color:${topicColor[t]||'#6B7280'}40">
                        <span class="topic-dot topic-dot-${t.toLowerCase()}"></span>${t}</span>`).join('');
                return `<a href="https://doi.org/${cleanDoi}" target="_blank" class="source-card ${srcCls}">
                    <div class="source-header">
                        <i class="ph ph-database"></i>
                        <span class="source-title" title="${d.title}">${d.title||cleanDoi}</span>
                        <span class="dataset-type-badge ${dm.cls}"><i class="ph ${dm.icon}"></i>${dm.label}</span>
                    </div>
                    ${ontoBadges?`<div class="ds-onto-row">${ontoBadges}</div>`:''}
                    <div class="source-metrics">
                        <div class="metric citations">
                            <img src="src/icons/citation.png" alt="Citations" style="width:18px;height:18px;opacity:.8;object-fit:contain;">
                            <div class="metric-info"><span class="metric-value">${d.citation_count.toLocaleString()}</span><span class="metric-label">Cites</span></div>
                        </div>
                        <div class="metric downloads">
                            <i class="ph ph-download-simple"></i>
                            <div class="metric-info"><span class="metric-value">${d.downloads_count.toLocaleString()}</span><span class="metric-label">Downs</span></div>
                        </div>
                        <div class="metric views">
                            <i class="ph ph-eye"></i>
                            <div class="metric-info"><span class="metric-value">${d.total_views.toLocaleString()}</span><span class="metric-label">Views</span></div>
                        </div>
                    </div>
                </a>`;
            }).join('')}</div>`;

        panelEl.innerHTML = `
            <div class="ds-filters-panel" style="margin-bottom:16px;padding:14px 16px;background:var(--background);border-radius:var(--radius-md);border:1px solid var(--border)">
                <div class="ds-filter-row" style="margin-bottom:10px">
                    <span class="ds-filter-label">Type</span>
                    <div class="ds-filter-group">${typeFilters}</div>
                </div>
                <div class="ds-filter-row" style="margin-bottom:0">
                    <span class="ds-filter-label">Topic</span>
                    <div class="ds-filter-group">${topicFilters}</div>
                </div>
            </div>
            <p style="font-size:12px;color:var(--text-muted);margin-bottom:12px">
                ${filtered.length} of ${groupData.length} datasets
            </p>
            ${cards}`;
    },
};

// Initialize after DOM load
document.addEventListener('DOMContentLoaded', () => {
    app.init();
});
