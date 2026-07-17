#[derive(Default)]
pub struct InMemoryRedisStore {
    checkpoint: Option<Checkpoint>,
    updates: VecDeque<ChainUpdate>,
    max_updates: usize,
    dedup: HashSet<String>,
    writer_lease: Option<(String, Instant)>,
    required_lease_owner: Option<String>,
}

impl InMemoryRedisStore {
    /// Creates a bounded deterministic store for tests or embedded callers.
    pub fn new(max_updates: usize) -> Self {
        Self {
            max_updates,
            dedup: HashSet::new(),
            ..Default::default()
        }
    }
}

impl CheckpointStore for InMemoryRedisStore {
    fn load(&self) -> Option<Checkpoint> {
        self.checkpoint.clone()
    }
    fn commit(&mut self, checkpoint: Checkpoint, updates: Vec<ChainUpdate>) -> Result<(), String> {
        if self.max_updates == 0 {
            return Err("update stream capacity must be non-zero".into());
        }
        if let Some(required_owner) = &self.required_lease_owner {
            let now = Instant::now();
            if !self
                .writer_lease
                .as_ref()
                .is_some_and(|(current_owner, expires_at)| {
                    current_owner == required_owner && *expires_at > now
                })
            {
                return Err("writer lease lost before checkpoint commit".into());
            }
        }
        self.checkpoint = Some(checkpoint);
        for update in updates {
            let identity = update_identity(&update);
            if self.dedup.insert(identity) {
                self.updates.push_back(update);
                while self.updates.len() > self.max_updates {
                    self.updates.pop_front();
                }
            }
        }
        Ok(())
    }
    fn updates(&self) -> Vec<ChainUpdate> {
        self.updates.iter().cloned().collect()
    }
    fn acquire_writer_lease(&mut self, owner: &str, ttl: Duration) -> Result<bool, String> {
        if ttl.is_zero() {
            return Err("writer lease TTL must be non-zero".into());
        }
        let now = Instant::now();
        if self
            .writer_lease
            .as_ref()
            .is_some_and(|(_, expires_at)| *expires_at > now)
        {
            return Ok(false);
        }
        self.writer_lease = Some((owner.to_owned(), now + ttl));
        Ok(true)
    }
    fn renew_writer_lease(&mut self, owner: &str, ttl: Duration) -> Result<bool, String> {
        if ttl.is_zero() {
            return Err("writer lease TTL must be non-zero".into());
        }
        let now = Instant::now();
        let Some((current_owner, expires_at)) = &mut self.writer_lease else {
            return Ok(false);
        };
        if *expires_at <= now {
            self.writer_lease = None;
            return Ok(false);
        }
        if current_owner != owner {
            return Ok(false);
        }
        *expires_at = now + ttl;
        Ok(true)
    }
    fn release_writer_lease(&mut self, owner: &str) -> Result<(), String> {
        if self
            .writer_lease
            .as_ref()
            .is_some_and(|(current_owner, _)| current_owner == owner)
        {
            self.writer_lease = None;
        }
        Ok(())
    }
    fn configure_writer_lease(&mut self, owner: Option<&str>) {
        self.required_lease_owner = owner.map(str::to_owned);
    }
}
