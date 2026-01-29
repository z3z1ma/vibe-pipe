import type {
  LoginRequest,
  RegisterRequest,
  AuthResponse,
  User,
  PipelineListResponse,
  AssetListResponse,
} from '../types/api';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://127.0.0.1:8000/api/v1';

interface ApiError extends Error {
  error: string;
  message: string;
  status: number;
  detail?: Record<string, unknown>;
}

function createApiError(
  error: string,
  message: string,
  status: number,
  detail?: Record<string, unknown>
): ApiError {
  const err = new Error(message) as ApiError;
  err.error = error;
  err.message = message;
  err.status = status;
  err.detail = detail;
  err.name = 'ApiError';
  return err;
}

async function fetchApi<T>(
  endpoint: string,
  options?: RequestInit
): Promise<T> {
  const token = localStorage.getItem('access_token');

  const headers: HeadersInit = {
    'Content-Type': 'application/json',
    ...(token && { Authorization: `Bearer ${token}` }),
    ...(options?.headers || {}),
  };

  const response = await fetch(`${API_BASE_URL}${endpoint}`, {
    ...options,
    headers,
  });

  if (!response.ok) {
    const data = await response.json();
    throw createApiError(
      data.error || 'unknown_error',
      data.message || 'An error occurred',
      response.status,
      data.detail
    );
  }

  return response.json();
}

export const api = {
  // Auth endpoints
  login: async (data: LoginRequest): Promise<AuthResponse> => {
    const formData = new URLSearchParams();
    formData.append('username', data.username);
    formData.append('password', data.password);

    const response = await fetch(`${API_BASE_URL}/auth/login`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: formData,
    });

    if (!response.ok) {
      const errorData = await response.json();
      throw createApiError(
        errorData.error || 'invalid_credentials',
        errorData.message || 'Login failed',
        response.status
      );
    }

    return response.json();
  },

  register: async (data: RegisterRequest): Promise<User> => {
    return fetchApi<User>('/auth/register', {
      method: 'POST',
      body: JSON.stringify(data),
    });
  },

  logout: async (): Promise<void> => {
    await fetchApi<void>('/auth/logout', { method: 'POST' });
  },

  refreshToken: async (refreshToken: string): Promise<AuthResponse> => {
    const response = await fetch(`${API_BASE_URL}/auth/refresh`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ refresh_token: refreshToken }),
    });

    if (!response.ok) {
      const errorData = await response.json();
      throw createApiError(
        errorData.error || 'invalid_token',
        errorData.message || 'Token refresh failed',
        response.status
      );
    }

    return response.json();
  },

  getCurrentUser: async (): Promise<User> => {
    return fetchApi<User>('/auth/me');
  },

  // Pipeline endpoints
  getPipelines: async (
    page: number = 1,
    pageSize: number = 50,
    status?: string
  ): Promise<PipelineListResponse> => {
    const params = new URLSearchParams({
      page: page.toString(),
      page_size: pageSize.toString(),
      ...(status && { status }),
    });

    return fetchApi<PipelineListResponse>(`/pipelines?${params}`);
  },

  runPipeline: async (pipelineId: string): Promise<{ run_id: string }> => {
    return fetchApi<{ run_id: string }>(`/pipelines/${pipelineId}/run`, {
      method: 'POST',
    });
  },

  // Asset endpoints
  getAssets: async (
    page: number = 1,
    pageSize: number = 50
  ): Promise<AssetListResponse> => {
    const params = new URLSearchParams({
      page: page.toString(),
      page_size: pageSize.toString(),
    });

    return fetchApi<AssetListResponse>(`/pipelines/assets?${params}`);
  },
};

export { createApiError as ApiError };
