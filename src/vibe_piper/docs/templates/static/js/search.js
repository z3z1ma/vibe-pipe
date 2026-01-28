/**
 * Client-side search functionality for Vibe Piper documentation.
 */

class DocumentationSearch {
    constructor(searchInputId, resultsContainerId) {
        this.searchInput = document.getElementById(searchInputId);
        this.resultsContainer = document.getElementById(resultsContainerId);
        this.searchIndex = null;
        this.debounceTimer = null;

        this.init();
    }

    async init() {
        // Load search index
        try {
            const response = await fetch('../static/js/search-index.json');
            this.searchIndex = await response.json();
        } catch (error) {
            console.error('Failed to load search index:', error);
        }

        // Set up event listeners
        if (this.searchInput) {
            this.searchInput.addEventListener('input', (e) => {
                this.handleSearch(e.target.value);
            });
        }
    }

    handleSearch(query) {
        // Clear previous timer
        if (this.debounceTimer) {
            clearTimeout(this.debounceTimer);
        }

        // Debounce search
        this.debounceTimer = setTimeout(() => {
            this.performSearch(query);
        }, 300);
    }

    performSearch(query) {
        if (!query || query.trim().length < 2) {
            this.clearResults();
            return;
        }

        if (!this.searchIndex) {
            return;
        }

        const searchTerm = query.toLowerCase().trim();
        const results = [];

        // Search assets
        if (this.searchIndex.assets) {
            for (const asset of this.searchIndex.assets) {
                const score = this.calculateScore(asset, searchTerm);
                if (score > 0) {
                    results.push({
                        ...asset,
                        type: 'asset',
                        score: score
                    });
                }
            }
        }

        // Search schemas
        if (this.searchIndex.schemas) {
            for (const schema of this.searchIndex.schemas) {
                const score = this.calculateScore(schema, searchTerm);
                if (score > 0) {
                    results.push({
                        ...schema,
                        type: 'schema',
                        score: score
                    });
                }
            }
        }

        // Sort by score
        results.sort((a, b) => b.score - a.score);

        // Display results
        this.displayResults(results.slice(0, 10)); // Limit to 10 results
    }

    calculateScore(item, searchTerm) {
        let score = 0;
        const name = item.name.toLowerCase();
        const description = (item.description || '').toLowerCase();

        // Exact name match gets highest score
        if (name === searchTerm) {
            score += 100;
        }
        // Name starts with search term
        else if (name.startsWith(searchTerm)) {
            score += 50;
        }
        // Name contains search term
        else if (name.includes(searchTerm)) {
            score += 25;
        }

        // Description contains search term
        if (description.includes(searchTerm)) {
            score += 10;
        }

        // Word boundary matches
        const words = searchTerm.split(' ');
        for (const word of words) {
            if (name.includes(word)) {
                score += 5;
            }
            if (description.includes(word)) {
                score += 2;
            }
        }

        return score;
    }

    displayResults(results) {
        if (!this.resultsContainer) {
            return;
        }

        if (results.length === 0) {
            this.resultsContainer.innerHTML = `
                <div class="no-results">
                    No results found
                </div>
            `;
            return;
        }

        const html = results.map(result => `
            <div class="search-result" data-score="${result.score}">
                <a href="${result.url}">
                    <div class="result-title">
                        <span class="result-type result-type-${result.type}">${result.type}</span>
                        <span class="result-name">${this.highlightMatch(result.name, this.searchInput.value)}</span>
                    </div>
                    ${result.description ? `<div class="result-description">${this.highlightMatch(result.description, this.searchInput.value, 100)}</div>` : ''}
                </a>
            </div>
        `).join('');

        this.resultsContainer.innerHTML = html;
    }

    highlightMatch(text, query, maxLength = null) {
        if (!query) {
            return maxLength ? text.substring(0, maxLength) + (text.length > maxLength ? '...' : '') : text;
        }

        let displayText = maxLength && text.length > maxLength
            ? text.substring(0, maxLength) + '...'
            : text;

        const regex = new RegExp(`(${query.split(' ').join('|')})`, 'gi');
        return displayText.replace(regex, '<mark>$1</mark>');
    }

    clearResults() {
        if (this.resultsContainer) {
            this.resultsContainer.innerHTML = '';
        }
    }
}

// Initialize search when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        new DocumentationSearch('search-input', 'search-results');
    });
} else {
    new DocumentationSearch('search-input', 'search-results');
}

// Also export for potential module usage
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { DocumentationSearch };
}
