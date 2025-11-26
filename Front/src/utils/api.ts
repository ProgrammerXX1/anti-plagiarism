// API service for backend communication

const API_BASE_URL = 'http://192.168.142.47:8002';

// Types based on actual API response
export type DocumentDetail = {
  doc_id: string;
  score: number;
  J9: number;
  C9: number;
  J13: number;
  C13: number;
  J5: number;
  C5: number;
  cand_hits: number;
  hamming_simhash: number;
  minhash_sim_est: number;
  matching_fragments: Array<{
    start: number;
    end: number;
    text: string;
  }>;
};

export type DocumentMatch = {
  doc_id: string;
  max_score: number;
  originality_pct: number;
  decision: string;
  details: DocumentDetail;
};

export type SearchResponse = {
  hits_total: number;
  docs_found: number;
  documents: DocumentMatch[];
};

export type UploadResponse = {
  doc_id: string;
  bytes: number;
};

export type BuildIndexResponse = {
  index_path: string;
  docs: number;
  k5: number;
  k9: number;
  k13: number;
};

export type CorpusItem = {
  doc_id: string;
  line_no?: number;
  chars: number;
  tokens: number;
  preview?: string;
};

export type CorpusListResponse = {
  total: number;
  offset: number;
  limit: number;
  items: CorpusItem[];
};

// Upload file to corpus
export async function uploadFile(
  file: File,
  normalize: boolean = true
): Promise<UploadResponse> {
  const formData = new FormData();
  formData.append('file', file);

  const response = await fetch(
    `${API_BASE_URL}/api/upload?normalize=${normalize}`,
    {
      method: 'POST',
      body: formData,
    }
  );

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Upload failed' }));
    throw new Error(error.detail || 'Failed to upload file');
  }

  return response.json();
}

// Build index from corpus
export async function buildIndex(): Promise<BuildIndexResponse> {
  const response = await fetch(`${API_BASE_URL}/api/build`, {
    method: 'POST',
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Build failed' }));
    throw new Error(error.detail || 'Failed to build index');
  }

  return response.json();
}

// Reset all data
export async function resetAll(): Promise<{ status: string; removed: string[] }> {
  const response = await fetch(`${API_BASE_URL}/api/reset`, {
    method: 'DELETE',
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Reset failed' }));
    throw new Error(error.detail || 'Failed to reset data');
  }

  return response.json();
}

// Get corpus list
export async function getCorpusList(
  offset: number = 0,
  limit: number = 50,
  reverse: boolean = true,
  preview: boolean = true,
  maxPreviewChars: number = 160
): Promise<CorpusListResponse> {
  const params = new URLSearchParams({
    offset: offset.toString(),
    limit: limit.toString(),
    reverse: reverse.toString(),
    preview: preview.toString(),
    max_preview_chars: maxPreviewChars.toString(),
  });

  const response = await fetch(`${API_BASE_URL}/api/corpus/list?${params}`);

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Failed to fetch corpus' }));
    throw new Error(error.detail || 'Failed to fetch corpus list');
  }

  return response.json();
}

// Search - always uses top: 5
export async function search(query: string): Promise<SearchResponse> {
  const response = await fetch(`${API_BASE_URL}/api/search`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ 
      query: query,
      top: 5 
    }),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Search failed' }));
    throw new Error(error.detail || 'Search failed');
  }

  return response.json();
}
