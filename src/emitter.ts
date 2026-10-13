/**
 * A tiny, platform-neutral EventEmitter with the subset of `node:events`'
 * API this SDK (and its callers) use: on/addListener/once/off/
 * removeListener/removeAllListeners/emit/listenerCount/listeners. Unlike
 * Node's, emitting `"error"` with no listener is a no-op, never a throw.
 * `node:events`' `once(emitter, name)` helper works against it (it only
 * duck-types `once`/`removeListener`).
 */

type Listener = (...args: any[]) => unknown;

interface Entry {
  fn: Listener;
  once: boolean;
}

export class Emitter {
  #events = new Map<string, Entry[]>();

  on(event: string, listener: Listener): this {
    return this.add(event, listener, false);
  }

  addListener(event: string, listener: Listener): this {
    return this.add(event, listener, false);
  }

  once(event: string, listener: Listener): this {
    return this.add(event, listener, true);
  }

  off(event: string, listener: Listener): this {
    const list = this.#events.get(event);
    if (!list) return this;
    // Last registration first, like Node's removeListener.
    for (let i = list.length - 1; i >= 0; i--) {
      if (list[i]!.fn === listener) {
        list.splice(i, 1);
        break;
      }
    }
    if (list.length === 0) this.#events.delete(event);
    return this;
  }

  removeListener(event: string, listener: Listener): this {
    return this.off(event, listener);
  }

  removeAllListeners(event?: string): this {
    if (event === undefined) this.#events.clear();
    else this.#events.delete(event);
    return this;
  }

  /** Calls every listener synchronously, in registration order, over a snapshot. Returns whether any listener was registered. A listener's throw propagates. */
  emit(event: string, ...args: unknown[]): boolean {
    const list = this.#events.get(event);
    if (!list || list.length === 0) return false;
    for (const entry of [...list]) {
      if (entry.once) this.dropEntry(event, entry);
      entry.fn.apply(this, args);
    }
    return true;
  }

  listenerCount(event: string): number {
    return this.#events.get(event)?.length ?? 0;
  }

  listeners(event: string): Listener[] {
    return (this.#events.get(event) ?? []).map((e) => e.fn);
  }

  /** Entries as {fn, once}; callers that invoke listeners themselves must drop once-entries first (dropEntry). */
  protected rawEntries(event: string): ReadonlyArray<Readonly<Entry>> {
    return [...(this.#events.get(event) ?? [])];
  }

  /** Removes exactly this entry (by identity), if still registered. */
  protected dropEntry(event: string, entry: Readonly<Entry>): void {
    const list = this.#events.get(event);
    if (!list) return;
    const i = list.indexOf(entry as Entry);
    if (i >= 0) list.splice(i, 1);
    if (list.length === 0) this.#events.delete(event);
  }

  private add(event: string, fn: Listener, once: boolean): this {
    let list = this.#events.get(event);
    if (!list) {
      list = [];
      this.#events.set(event, list);
    }
    list.push({ fn, once });
    return this;
  }
}
