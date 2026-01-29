export interface User {
  id: string;
  email: string;
  username: string;
  full_name: string | null;
  is_active: boolean;
  created_at: string;
}

export interface LoginRequest {
  username: string;
  password: string;
}

export interface RegisterRequest {
  email: string;
  username: string;
  password: string;
  full_name?: string;
}

export interface AuthResponse {
  access_token: string;
  refresh_token: string;
  token_type: string;
  expires_in: number;
}

export interface Pipeline {
  id: string;
  name: string;
  description: string | null;
  status: string;
  created_at: string;
  updated_at: string | null;
  last_run_at: string | null;
  last_run_status: string | null;
  schedule: string | null;
  is_active: boolean;
}

export interface PipelineListResponse {
  pipelines: Pipeline[];
  total: number;
  page: number;
  page_size: number;
}

export interface Asset {
  id: string;
  name: string;
  type: string;
  location: string;
  created_at: string;
  updated_at: string | null;
  size_bytes: number | null;
  metadata: Record<string, unknown> | null;
}

export interface AssetListResponse {
  assets: Asset[];
  total: number;
  page: number;
  page_size: number;
}
