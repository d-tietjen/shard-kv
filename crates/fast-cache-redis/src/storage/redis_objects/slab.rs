use super::*;

impl<T> ObjectSlab<T> {
    pub(super) fn insert(&mut self, value: T) -> SlotId {
        if let Some(slot) = self.free.pop() {
            self.entries[slot] = Some(value);
            slot
        } else {
            let slot = self.entries.len();
            self.entries.push(Some(value));
            slot
        }
    }

    pub(super) fn get(&self, slot: SlotId) -> Option<&T> {
        self.entries.get(slot).and_then(Option::as_ref)
    }

    pub(super) fn get_mut(&mut self, slot: SlotId) -> Option<&mut T> {
        self.entries.get_mut(slot).and_then(Option::as_mut)
    }

    pub(super) fn remove(&mut self, slot: SlotId) -> Option<T> {
        let value = self.entries.get_mut(slot)?.take();
        if value.is_some() {
            self.free.push(slot);
        }
        value
    }
}
