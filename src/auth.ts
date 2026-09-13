import { createClient } from "@supabase/supabase-js";

export interface AuthenticatedUser {
  id: string;
}

export type Authenticate = (
  accessToken: string,
) => Promise<AuthenticatedUser | null>;

export function createSupabaseAuthenticator(
  url: string,
  anonKey: string,
): Authenticate {
  const supabase = createClient(url, anonKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  return async (accessToken) => {
    const { data, error } = await supabase.auth.getUser(accessToken);
    if (error || !data.user) return null;
    return { id: data.user.id };
  };
}
