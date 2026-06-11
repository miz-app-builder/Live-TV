-- ════════════════════════════════════════════════════
--  MIZ Live TV — Supabase Database Setup
--  Run this in: Supabase Dashboard → SQL Editor
-- ════════════════════════════════════════════════════

-- 1) profiles table — stores user roles
CREATE TABLE IF NOT EXISTS public.profiles (
  id        UUID REFERENCES auth.users(id) ON DELETE CASCADE PRIMARY KEY,
  role      TEXT DEFAULT 'member' CHECK (role IN ('member', 'admin')),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can read own profile" ON public.profiles
  FOR SELECT USING (auth.uid() = id);

CREATE POLICY "Users can insert own profile" ON public.profiles
  FOR INSERT WITH CHECK (auth.uid() = id);

-- 2) channel_visibility table — controls which channels guests see
CREATE TABLE IF NOT EXISTS public.channel_visibility (
  channel_id        INTEGER PRIMARY KEY,
  visible_to_guests BOOLEAN DEFAULT TRUE
);

ALTER TABLE public.channel_visibility ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Anyone can read channel visibility" ON public.channel_visibility
  FOR SELECT USING (TRUE);

CREATE POLICY "Service role manages visibility" ON public.channel_visibility
  FOR ALL USING (TRUE);

-- 3) Trigger: auto-create profile row when a user signs up
CREATE OR REPLACE FUNCTION public.handle_new_user()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO public.profiles (id, role)
  VALUES (NEW.id, 'member')
  ON CONFLICT (id) DO NOTHING;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
CREATE TRIGGER on_auth_user_created
  AFTER INSERT ON auth.users
  FOR EACH ROW EXECUTE PROCEDURE public.handle_new_user();

-- 4) channel_url_overrides table — admin-editable stream URLs
CREATE TABLE IF NOT EXISTS public.channel_url_overrides (
  channel_id  INTEGER PRIMARY KEY,
  stream_url  TEXT NOT NULL,
  updated_at  TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE public.channel_url_overrides ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Service role manages overrides" ON public.channel_url_overrides FOR ALL USING (TRUE);

-- 5) app_config table — key-value settings (e.g. guest_limit_minutes)
CREATE TABLE IF NOT EXISTS public.app_config (
  key         TEXT PRIMARY KEY,
  value       TEXT NOT NULL,
  updated_at  TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE public.app_config ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Anyone can read config" ON public.app_config FOR SELECT USING (TRUE);
CREATE POLICY "Service role manages config" ON public.app_config FOR ALL USING (TRUE);

-- ════════════════════════════════════════════════════
--  After signing up, make yourself admin:
--  (replace your@email.com with your actual email)
-- ════════════════════════════════════════════════════
-- UPDATE public.profiles
-- SET role = 'admin'
-- WHERE id = (SELECT id FROM auth.users WHERE email = 'your@email.com');
