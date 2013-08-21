--- a/Lib/bsddb/__init__.py
+++ b/Lib/bsddb/__init__.py
@@ -33,18 +33,34 @@
 #----------------------------------------------------------------------
 
 
-"""Support for BerkeleyDB 3.3 through 4.4 with a simple interface.
+"""Support for Berkeley DB 4.3 through 5.3 with a simple interface.
 
 For the full featured object oriented interface use the bsddb.db module
-instead.  It mirrors the Sleepycat BerkeleyDB C API.
+instead.  It mirrors the Oracle Berkeley DB C API.
 """
 
+import sys
+absolute_import = (sys.version_info[0] >= 3)
+
+if (sys.version_info >= (2, 6)) and (sys.version_info < (3, 0)) :
+    import warnings
+    if sys.py3kwarning and (__name__ != 'bsddb3') :
+        warnings.warnpy3k("in 3.x, the bsddb module has been removed; "
+                          "please use the pybsddb project instead",
+                          DeprecationWarning, 2)
+    warnings.filterwarnings("ignore", ".*CObject.*", DeprecationWarning,
+                            "bsddb.__init__")
+
 try:
     if __name__ == 'bsddb3':
         # import _pybsddb binary as it should be the more recent version from
         # a standalone pybsddb addon package than the version included with
         # python as bsddb._bsddb.
-        import _pybsddb
+        if absolute_import :
+            # Because this syntaxis is not valid before Python 2.5
+            exec("from . import _pybsddb")
+        else :
+            import _pybsddb
         _bsddb = _pybsddb
         from bsddb3.dbutils import DeadlockWrap as _DeadlockWrap
     else:
@@ -66,14 +82,16 @@
 
 import sys, os
 
-# for backwards compatibility with python versions older than 2.3, the
-# iterator interface is dynamically defined and added using a mixin
-# class.  old python can't tokenize it due to the yield keyword.
-if sys.version >= '2.3':
+from weakref import ref
+
+if sys.version_info < (2, 6) :
     import UserDict
-    from weakref import ref
-    exec """
-class _iter_mixin(UserDict.DictMixin):
+    MutableMapping = UserDict.DictMixin
+else :
+    import collections
+    MutableMapping = collections.MutableMapping
+
+class _iter_mixin(MutableMapping):
     def _make_iter_cursor(self):
         cur = _DeadlockWrap(self.db.cursor)
         key = id(cur)
@@ -87,67 +105,88 @@
         return lambda ref: self._cursor_refs.pop(key, None)
 
     def __iter__(self):
+        self._kill_iteration = False
+        self._in_iter += 1
         try:
-            cur = self._make_iter_cursor()
+            try:
+                cur = self._make_iter_cursor()
 
-            # FIXME-20031102-greg: race condition.  cursor could
-            # be closed by another thread before this call.
+                # FIXME-20031102-greg: race condition.  cursor could
+                # be closed by another thread before this call.
 
-            # since we're only returning keys, we call the cursor
-            # methods with flags=0, dlen=0, dofs=0
-            key = _DeadlockWrap(cur.first, 0,0,0)[0]
-            yield key
+                # since we're only returning keys, we call the cursor
+                # methods with flags=0, dlen=0, dofs=0
+                key = _DeadlockWrap(cur.first, 0,0,0)[0]
+                yield key
+
+                next = getattr(cur, "next")
+                while 1:
+                    try:
+                        key = _DeadlockWrap(next, 0,0,0)[0]
+                        yield key
+                    except _bsddb.DBCursorClosedError:
+                        if self._kill_iteration:
+                            raise RuntimeError('Database changed size '
+                                               'during iteration.')
+                        cur = self._make_iter_cursor()
+                        # FIXME-20031101-greg: race condition.  cursor could
+                        # be closed by another thread before this call.
+                        _DeadlockWrap(cur.set, key,0,0,0)
+                        next = getattr(cur, "next")
+            except _bsddb.DBNotFoundError:
+                pass
+            except _bsddb.DBCursorClosedError:
+                # the database was modified during iteration.  abort.
+                pass
+# When Python 2.4 not supported in bsddb3, we can change this to "finally"
+        except :
+            self._in_iter -= 1
+            raise
 
-            next = cur.next
-            while 1:
-                try:
-                    key = _DeadlockWrap(next, 0,0,0)[0]
-                    yield key
-                except _bsddb.DBCursorClosedError:
-                    cur = self._make_iter_cursor()
-                    # FIXME-20031101-greg: race condition.  cursor could
-                    # be closed by another thread before this call.
-                    _DeadlockWrap(cur.set, key,0,0,0)
-                    next = cur.next
-        except _bsddb.DBNotFoundError:
-            return
-        except _bsddb.DBCursorClosedError:
-            # the database was modified during iteration.  abort.
-            return
+        self._in_iter -= 1
 
     def iteritems(self):
         if not self.db:
             return
+        self._kill_iteration = False
+        self._in_iter += 1
         try:
-            cur = self._make_iter_cursor()
+            try:
+                cur = self._make_iter_cursor()
 
-            # FIXME-20031102-greg: race condition.  cursor could
-            # be closed by another thread before this call.
+                # FIXME-20031102-greg: race condition.  cursor could
+                # be closed by another thread before this call.
 
-            kv = _DeadlockWrap(cur.first)
-            key = kv[0]
-            yield kv
+                kv = _DeadlockWrap(cur.first)
+                key = kv[0]
+                yield kv
+
+                next = getattr(cur, "next")
+                while 1:
+                    try:
+                        kv = _DeadlockWrap(next)
+                        key = kv[0]
+                        yield kv
+                    except _bsddb.DBCursorClosedError:
+                        if self._kill_iteration:
+                            raise RuntimeError('Database changed size '
+                                               'during iteration.')
+                        cur = self._make_iter_cursor()
+                        # FIXME-20031101-greg: race condition.  cursor could
+                        # be closed by another thread before this call.
+                        _DeadlockWrap(cur.set, key,0,0,0)
+                        next = getattr(cur, "next")
+            except _bsddb.DBNotFoundError:
+                pass
+            except _bsddb.DBCursorClosedError:
+                # the database was modified during iteration.  abort.
+                pass
+# When Python 2.4 not supported in bsddb3, we can change this to "finally"
+        except :
+            self._in_iter -= 1
+            raise
 
-            next = cur.next
-            while 1:
-                try:
-                    kv = _DeadlockWrap(next)
-                    key = kv[0]
-                    yield kv
-                except _bsddb.DBCursorClosedError:
-                    cur = self._make_iter_cursor()
-                    # FIXME-20031101-greg: race condition.  cursor could
-                    # be closed by another thread before this call.
-                    _DeadlockWrap(cur.set, key,0,0,0)
-                    next = cur.next
-        except _bsddb.DBNotFoundError:
-            return
-        except _bsddb.DBCursorClosedError:
-            # the database was modified during iteration.  abort.
-            return
-"""
-else:
-    class _iter_mixin: pass
+        self._in_iter -= 1
 
 
 class _DBWithCursor(_iter_mixin):
@@ -176,6 +215,8 @@
         # a collection of all DBCursor objects currently allocated
         # by the _iter_mixin interface.
         self._cursor_refs = {}
+        self._in_iter = 0
+        self._kill_iteration = False
 
     def __del__(self):
         self.close()
@@ -188,7 +229,7 @@
                 self.saved_dbc_key = None
 
     # This method is needed for all non-cursor DB calls to avoid
-    # BerkeleyDB deadlocks (due to being opened with DB_INIT_LOCK
+    # Berkeley DB deadlocks (due to being opened with DB_INIT_LOCK
     # and DB_THREAD to be thread safe) when intermixing database
     # operations that use the cursor internally with those that don't.
     def _closeCursors(self, save=1):
@@ -218,6 +259,12 @@
         self._checkOpen()
         return _DeadlockWrap(lambda: len(self.db))  # len(self.db)
 
+    if sys.version_info >= (2, 6) :
+        def __repr__(self) :
+            if self.isOpen() :
+                return repr(dict(_DeadlockWrap(self.db.items)))
+            return repr(dict())
+
     def __getitem__(self, key):
         self._checkOpen()
         return _DeadlockWrap(lambda: self.db[key])  # self.db[key]
@@ -225,6 +272,8 @@
     def __setitem__(self, key, value):
         self._checkOpen()
         self._closeCursors()
+        if self._in_iter and key not in self:
+            self._kill_iteration = True
         def wrapF():
             self.db[key] = value
         _DeadlockWrap(wrapF)  # self.db[key] = value
@@ -232,6 +281,8 @@
     def __delitem__(self, key):
         self._checkOpen()
         self._closeCursors()
+        if self._in_iter and key in self:
+            self._kill_iteration = True
         def wrapF():
             del self.db[key]
         _DeadlockWrap(wrapF)  # del self.db[key]
@@ -260,12 +311,15 @@
         self._checkCursor()
         return _DeadlockWrap(self.dbc.set_range, key)
 
-    def next(self):
+    def next(self):  # Renamed by "2to3"
         self._checkOpen()
         self._checkCursor()
-        rv = _DeadlockWrap(self.dbc.next)
+        rv = _DeadlockWrap(getattr(self.dbc, "next"))
         return rv
 
+    if sys.version_info[0] >= 3 :  # For "2to3" conversion
+        next = __next__
+
     def previous(self):
         self._checkOpen()
         self._checkCursor()
@@ -372,7 +426,7 @@
     elif flag == 'n':
         flags = db.DB_CREATE
         #flags = db.DB_CREATE | db.DB_TRUNCATE
-        # we used db.DB_TRUNCATE flag for this before but BerkeleyDB
+        # we used db.DB_TRUNCATE flag for this before but Berkeley DB
         # 4.2.52 changed to disallowed truncate with txn environments.
         if file is not None and os.path.isfile(file):
             os.unlink(file)
@@ -385,16 +439,16 @@
 
 # This is a silly little hack that allows apps to continue to use the
 # DB_THREAD flag even on systems without threads without freaking out
-# BerkeleyDB.
+# Berkeley DB.
 #
 # This assumes that if Python was built with thread support then
-# BerkeleyDB was too.
+# Berkeley DB was too.
 
 try:
-    import thread
-    del thread
-    if db.version() < (3, 3, 0):
-        db.DB_THREAD = 0
+    # 2to3 automatically changes "import thread" to "import _thread"
+    import thread as T
+    del T
+
 except ImportError:
     db.DB_THREAD = 0
 
--- a/Lib/bsddb/db.py
+++ b/Lib/bsddb/db.py
@@ -37,15 +37,24 @@
 # case we ever want to augment the stuff in _db in any way.  For now
 # it just simply imports everything from _db.
 
-if __name__.startswith('bsddb3.'):
-    # import _pybsddb binary as it should be the more recent version from
-    # a standalone pybsddb addon package than the version included with
-    # python as bsddb._bsddb.
-    from _pybsddb import *
-    from _pybsddb import __version__
-else:
-    from _bsddb import *
-    from _bsddb import __version__
+import sys
+absolute_import = (sys.version_info[0] >= 3)
 
-if version() < (3, 2, 0):
-    raise ImportError, "correct BerkeleyDB symbols not found.  Perhaps python was statically linked with an older version?"
+if not absolute_import :
+    if __name__.startswith('bsddb3.') :
+        # import _pybsddb binary as it should be the more recent version from
+        # a standalone pybsddb addon package than the version included with
+        # python as bsddb._bsddb.
+        from _pybsddb import *
+        from _pybsddb import __version__
+    else:
+        from _bsddb import *
+        from _bsddb import __version__
+else :
+    # Because this syntaxis is not valid before Python 2.5
+    if __name__.startswith('bsddb3.') :
+        exec("from ._pybsddb import *")
+        exec("from ._pybsddb import __version__")
+    else :
+        exec("from ._bsddb import *")
+        exec("from ._bsddb import __version__")
--- a/Lib/bsddb/dbobj.py
+++ b/Lib/bsddb/dbobj.py
@@ -21,102 +21,109 @@
 # added to _bsddb.c.
 #
 
-import db
-
-try:
-    from UserDict import DictMixin
-except ImportError:
-    # DictMixin is new in Python 2.3
-    class DictMixin: pass
+import sys
+absolute_import = (sys.version_info[0] >= 3)
+if absolute_import :
+    # Because this syntaxis is not valid before Python 2.5
+    exec("from . import db")
+else :
+    import db
+
+if sys.version_info < (2, 6) :
+    from UserDict import DictMixin as MutableMapping
+else :
+    import collections
+    MutableMapping = collections.MutableMapping
 
 class DBEnv:
     def __init__(self, *args, **kwargs):
-        self._cobj = apply(db.DBEnv, args, kwargs)
+        self._cobj = db.DBEnv(*args, **kwargs)
 
     def close(self, *args, **kwargs):
-        return apply(self._cobj.close, args, kwargs)
+        return self._cobj.close(*args, **kwargs)
     def open(self, *args, **kwargs):
-        return apply(self._cobj.open, args, kwargs)
+        return self._cobj.open(*args, **kwargs)
     def remove(self, *args, **kwargs):
-        return apply(self._cobj.remove, args, kwargs)
+        return self._cobj.remove(*args, **kwargs)
     def set_shm_key(self, *args, **kwargs):
-        return apply(self._cobj.set_shm_key, args, kwargs)
+        return self._cobj.set_shm_key(*args, **kwargs)
     def set_cachesize(self, *args, **kwargs):
-        return apply(self._cobj.set_cachesize, args, kwargs)
+        return self._cobj.set_cachesize(*args, **kwargs)
     def set_data_dir(self, *args, **kwargs):
-        return apply(self._cobj.set_data_dir, args, kwargs)
+        return self._cobj.set_data_dir(*args, **kwargs)
     def set_flags(self, *args, **kwargs):
-        return apply(self._cobj.set_flags, args, kwargs)
+        return self._cobj.set_flags(*args, **kwargs)
     def set_lg_bsize(self, *args, **kwargs):
-        return apply(self._cobj.set_lg_bsize, args, kwargs)
+        return self._cobj.set_lg_bsize(*args, **kwargs)
     def set_lg_dir(self, *args, **kwargs):
-        return apply(self._cobj.set_lg_dir, args, kwargs)
+        return self._cobj.set_lg_dir(*args, **kwargs)
     def set_lg_max(self, *args, **kwargs):
-        return apply(self._cobj.set_lg_max, args, kwargs)
+        return self._cobj.set_lg_max(*args, **kwargs)
     def set_lk_detect(self, *args, **kwargs):
-        return apply(self._cobj.set_lk_detect, args, kwargs)
+        return self._cobj.set_lk_detect(*args, **kwargs)
     if db.version() < (4,5):
         def set_lk_max(self, *args, **kwargs):
-            return apply(self._cobj.set_lk_max, args, kwargs)
+            return self._cobj.set_lk_max(*args, **kwargs)
     def set_lk_max_locks(self, *args, **kwargs):
-        return apply(self._cobj.set_lk_max_locks, args, kwargs)
+        return self._cobj.set_lk_max_locks(*args, **kwargs)
     def set_lk_max_lockers(self, *args, **kwargs):
-        return apply(self._cobj.set_lk_max_lockers, args, kwargs)
+        return self._cobj.set_lk_max_lockers(*args, **kwargs)
     def set_lk_max_objects(self, *args, **kwargs):
-        return apply(self._cobj.set_lk_max_objects, args, kwargs)
+        return self._cobj.set_lk_max_objects(*args, **kwargs)
     def set_mp_mmapsize(self, *args, **kwargs):
-        return apply(self._cobj.set_mp_mmapsize, args, kwargs)
+        return self._cobj.set_mp_mmapsize(*args, **kwargs)
     def set_timeout(self, *args, **kwargs):
-        return apply(self._cobj.set_timeout, args, kwargs)
+        return self._cobj.set_timeout(*args, **kwargs)
     def set_tmp_dir(self, *args, **kwargs):
-        return apply(self._cobj.set_tmp_dir, args, kwargs)
+        return self._cobj.set_tmp_dir(*args, **kwargs)
     def txn_begin(self, *args, **kwargs):
-        return apply(self._cobj.txn_begin, args, kwargs)
+        return self._cobj.txn_begin(*args, **kwargs)
     def txn_checkpoint(self, *args, **kwargs):
-        return apply(self._cobj.txn_checkpoint, args, kwargs)
+        return self._cobj.txn_checkpoint(*args, **kwargs)
     def txn_stat(self, *args, **kwargs):
-        return apply(self._cobj.txn_stat, args, kwargs)
+        return self._cobj.txn_stat(*args, **kwargs)
     def set_tx_max(self, *args, **kwargs):
-        return apply(self._cobj.set_tx_max, args, kwargs)
+        return self._cobj.set_tx_max(*args, **kwargs)
     def set_tx_timestamp(self, *args, **kwargs):
-        return apply(self._cobj.set_tx_timestamp, args, kwargs)
+        return self._cobj.set_tx_timestamp(*args, **kwargs)
     def lock_detect(self, *args, **kwargs):
-        return apply(self._cobj.lock_detect, args, kwargs)
+        return self._cobj.lock_detect(*args, **kwargs)
     def lock_get(self, *args, **kwargs):
-        return apply(self._cobj.lock_get, args, kwargs)
+        return self._cobj.lock_get(*args, **kwargs)
     def lock_id(self, *args, **kwargs):
-        return apply(self._cobj.lock_id, args, kwargs)
+        return self._cobj.lock_id(*args, **kwargs)
     def lock_put(self, *args, **kwargs):
-        return apply(self._cobj.lock_put, args, kwargs)
+        return self._cobj.lock_put(*args, **kwargs)
     def lock_stat(self, *args, **kwargs):
-        return apply(self._cobj.lock_stat, args, kwargs)
+        return self._cobj.lock_stat(*args, **kwargs)
     def log_archive(self, *args, **kwargs):
-        return apply(self._cobj.log_archive, args, kwargs)
+        return self._cobj.log_archive(*args, **kwargs)
 
     def set_get_returns_none(self, *args, **kwargs):
-        return apply(self._cobj.set_get_returns_none, args, kwargs)
+        return self._cobj.set_get_returns_none(*args, **kwargs)
+
+    def log_stat(self, *args, **kwargs):
+        return self._cobj.log_stat(*args, **kwargs)
 
-    if db.version() >= (4,0):
-        def log_stat(self, *args, **kwargs):
-            return apply(self._cobj.log_stat, args, kwargs)
-
-    if db.version() >= (4,1):
-        def dbremove(self, *args, **kwargs):
-            return apply(self._cobj.dbremove, args, kwargs)
-        def dbrename(self, *args, **kwargs):
-            return apply(self._cobj.dbrename, args, kwargs)
-        def set_encrypt(self, *args, **kwargs):
-            return apply(self._cobj.set_encrypt, args, kwargs)
+    def dbremove(self, *args, **kwargs):
+        return self._cobj.dbremove(*args, **kwargs)
+    def dbrename(self, *args, **kwargs):
+        return self._cobj.dbrename(*args, **kwargs)
+    def set_encrypt(self, *args, **kwargs):
+        return self._cobj.set_encrypt(*args, **kwargs)
 
     if db.version() >= (4,4):
+        def fileid_reset(self, *args, **kwargs):
+            return self._cobj.fileid_reset(*args, **kwargs)
+
         def lsn_reset(self, *args, **kwargs):
-            return apply(self._cobj.lsn_reset, args, kwargs)
+            return self._cobj.lsn_reset(*args, **kwargs)
 
 
-class DB(DictMixin):
+class DB(MutableMapping):
     def __init__(self, dbenv, *args, **kwargs):
         # give it the proper DBEnv C object that its expecting
-        self._cobj = apply(db.DB, (dbenv._cobj,) + args, kwargs)
+        self._cobj = db.DB(*((dbenv._cobj,) + args), **kwargs)
 
     # TODO are there other dict methods that need to be overridden?
     def __len__(self):
@@ -128,127 +135,132 @@
     def __delitem__(self, arg):
         del self._cobj[arg]
 
+    if sys.version_info >= (2, 6) :
+        def __iter__(self) :
+            return self._cobj.__iter__()
+
     def append(self, *args, **kwargs):
-        return apply(self._cobj.append, args, kwargs)
+        return self._cobj.append(*args, **kwargs)
     def associate(self, *args, **kwargs):
-        return apply(self._cobj.associate, args, kwargs)
+        return self._cobj.associate(*args, **kwargs)
     def close(self, *args, **kwargs):
-        return apply(self._cobj.close, args, kwargs)
+        return self._cobj.close(*args, **kwargs)
     def consume(self, *args, **kwargs):
-        return apply(self._cobj.consume, args, kwargs)
+        return self._cobj.consume(*args, **kwargs)
     def consume_wait(self, *args, **kwargs):
-        return apply(self._cobj.consume_wait, args, kwargs)
+        return self._cobj.consume_wait(*args, **kwargs)
     def cursor(self, *args, **kwargs):
-        return apply(self._cobj.cursor, args, kwargs)
+        return self._cobj.cursor(*args, **kwargs)
     def delete(self, *args, **kwargs):
-        return apply(self._cobj.delete, args, kwargs)
+        return self._cobj.delete(*args, **kwargs)
     def fd(self, *args, **kwargs):
-        return apply(self._cobj.fd, args, kwargs)
+        return self._cobj.fd(*args, **kwargs)
     def get(self, *args, **kwargs):
-        return apply(self._cobj.get, args, kwargs)
+        return self._cobj.get(*args, **kwargs)
     def pget(self, *args, **kwargs):
-        return apply(self._cobj.pget, args, kwargs)
+        return self._cobj.pget(*args, **kwargs)
     def get_both(self, *args, **kwargs):
-        return apply(self._cobj.get_both, args, kwargs)
+        return self._cobj.get_both(*args, **kwargs)
     def get_byteswapped(self, *args, **kwargs):
-        return apply(self._cobj.get_byteswapped, args, kwargs)
+        return self._cobj.get_byteswapped(*args, **kwargs)
     def get_size(self, *args, **kwargs):
-        return apply(self._cobj.get_size, args, kwargs)
+        return self._cobj.get_size(*args, **kwargs)
     def get_type(self, *args, **kwargs):
-        return apply(self._cobj.get_type, args, kwargs)
+        return self._cobj.get_type(*args, **kwargs)
     def join(self, *args, **kwargs):
-        return apply(self._cobj.join, args, kwargs)
+        return self._cobj.join(*args, **kwargs)
     def key_range(self, *args, **kwargs):
-        return apply(self._cobj.key_range, args, kwargs)
+        return self._cobj.key_range(*args, **kwargs)
     def has_key(self, *args, **kwargs):
-        return apply(self._cobj.has_key, args, kwargs)
+        return self._cobj.has_key(*args, **kwargs)
     def items(self, *args, **kwargs):
-        return apply(self._cobj.items, args, kwargs)
+        return self._cobj.items(*args, **kwargs)
     def keys(self, *args, **kwargs):
-        return apply(self._cobj.keys, args, kwargs)
+        return self._cobj.keys(*args, **kwargs)
     def open(self, *args, **kwargs):
-        return apply(self._cobj.open, args, kwargs)
+        return self._cobj.open(*args, **kwargs)
     def put(self, *args, **kwargs):
-        return apply(self._cobj.put, args, kwargs)
+        return self._cobj.put(*args, **kwargs)
     def remove(self, *args, **kwargs):
-        return apply(self._cobj.remove, args, kwargs)
+        return self._cobj.remove(*args, **kwargs)
     def rename(self, *args, **kwargs):
-        return apply(self._cobj.rename, args, kwargs)
+        return self._cobj.rename(*args, **kwargs)
     def set_bt_minkey(self, *args, **kwargs):
-        return apply(self._cobj.set_bt_minkey, args, kwargs)
+        return self._cobj.set_bt_minkey(*args, **kwargs)
     def set_bt_compare(self, *args, **kwargs):
-        return apply(self._cobj.set_bt_compare, args, kwargs)
+        return self._cobj.set_bt_compare(*args, **kwargs)
     def set_cachesize(self, *args, **kwargs):
-        return apply(self._cobj.set_cachesize, args, kwargs)
+        return self._cobj.set_cachesize(*args, **kwargs)
+    def set_dup_compare(self, *args, **kwargs) :
+        return self._cobj.set_dup_compare(*args, **kwargs)
     def set_flags(self, *args, **kwargs):
-        return apply(self._cobj.set_flags, args, kwargs)
+        return self._cobj.set_flags(*args, **kwargs)
     def set_h_ffactor(self, *args, **kwargs):
-        return apply(self._cobj.set_h_ffactor, args, kwargs)
+        return self._cobj.set_h_ffactor(*args, **kwargs)
     def set_h_nelem(self, *args, **kwargs):
-        return apply(self._cobj.set_h_nelem, args, kwargs)
+        return self._cobj.set_h_nelem(*args, **kwargs)
     def set_lorder(self, *args, **kwargs):
-        return apply(self._cobj.set_lorder, args, kwargs)
+        return self._cobj.set_lorder(*args, **kwargs)
     def set_pagesize(self, *args, **kwargs):
-        return apply(self._cobj.set_pagesize, args, kwargs)
+        return self._cobj.set_pagesize(*args, **kwargs)
     def set_re_delim(self, *args, **kwargs):
-        return apply(self._cobj.set_re_delim, args, kwargs)
+        return self._cobj.set_re_delim(*args, **kwargs)
     def set_re_len(self, *args, **kwargs):
-        return apply(self._cobj.set_re_len, args, kwargs)
+        return self._cobj.set_re_len(*args, **kwargs)
     def set_re_pad(self, *args, **kwargs):
-        return apply(self._cobj.set_re_pad, args, kwargs)
+        return self._cobj.set_re_pad(*args, **kwargs)
     def set_re_source(self, *args, **kwargs):
-        return apply(self._cobj.set_re_source, args, kwargs)
+        return self._cobj.set_re_source(*args, **kwargs)
     def set_q_extentsize(self, *args, **kwargs):
-        return apply(self._cobj.set_q_extentsize, args, kwargs)
+        return self._cobj.set_q_extentsize(*args, **kwargs)
     def stat(self, *args, **kwargs):
-        return apply(self._cobj.stat, args, kwargs)
+        return self._cobj.stat(*args, **kwargs)
     def sync(self, *args, **kwargs):
-        return apply(self._cobj.sync, args, kwargs)
+        return self._cobj.sync(*args, **kwargs)
     def type(self, *args, **kwargs):
-        return apply(self._cobj.type, args, kwargs)
+        return self._cobj.type(*args, **kwargs)
     def upgrade(self, *args, **kwargs):
-        return apply(self._cobj.upgrade, args, kwargs)
+        return self._cobj.upgrade(*args, **kwargs)
     def values(self, *args, **kwargs):
-        return apply(self._cobj.values, args, kwargs)
+        return self._cobj.values(*args, **kwargs)
     def verify(self, *args, **kwargs):
-        return apply(self._cobj.verify, args, kwargs)
+        return self._cobj.verify(*args, **kwargs)
     def set_get_returns_none(self, *args, **kwargs):
-        return apply(self._cobj.set_get_returns_none, args, kwargs)
+        return self._cobj.set_get_returns_none(*args, **kwargs)
 
-    if db.version() >= (4,1):
-        def set_encrypt(self, *args, **kwargs):
-            return apply(self._cobj.set_encrypt, args, kwargs)
+    def set_encrypt(self, *args, **kwargs):
+        return self._cobj.set_encrypt(*args, **kwargs)
 
 
 class DBSequence:
     def __init__(self, *args, **kwargs):
-        self._cobj = apply(db.DBSequence, args, kwargs)
+        self._cobj = db.DBSequence(*args, **kwargs)
 
     def close(self, *args, **kwargs):
-        return apply(self._cobj.close, args, kwargs)
+        return self._cobj.close(*args, **kwargs)
     def get(self, *args, **kwargs):
-        return apply(self._cobj.get, args, kwargs)
+        return self._cobj.get(*args, **kwargs)
     def get_dbp(self, *args, **kwargs):
-        return apply(self._cobj.get_dbp, args, kwargs)
+        return self._cobj.get_dbp(*args, **kwargs)
     def get_key(self, *args, **kwargs):
-        return apply(self._cobj.get_key, args, kwargs)
+        return self._cobj.get_key(*args, **kwargs)
     def init_value(self, *args, **kwargs):
-        return apply(self._cobj.init_value, args, kwargs)
+        return self._cobj.init_value(*args, **kwargs)
     def open(self, *args, **kwargs):
-        return apply(self._cobj.open, args, kwargs)
+        return self._cobj.open(*args, **kwargs)
     def remove(self, *args, **kwargs):
-        return apply(self._cobj.remove, args, kwargs)
+        return self._cobj.remove(*args, **kwargs)
     def stat(self, *args, **kwargs):
-        return apply(self._cobj.stat, args, kwargs)
+        return self._cobj.stat(*args, **kwargs)
     def set_cachesize(self, *args, **kwargs):
-        return apply(self._cobj.set_cachesize, args, kwargs)
+        return self._cobj.set_cachesize(*args, **kwargs)
     def set_flags(self, *args, **kwargs):
-        return apply(self._cobj.set_flags, args, kwargs)
+        return self._cobj.set_flags(*args, **kwargs)
     def set_range(self, *args, **kwargs):
-        return apply(self._cobj.set_range, args, kwargs)
+        return self._cobj.set_range(*args, **kwargs)
     def get_cachesize(self, *args, **kwargs):
-        return apply(self._cobj.get_cachesize, args, kwargs)
+        return self._cobj.get_cachesize(*args, **kwargs)
     def get_flags(self, *args, **kwargs):
-        return apply(self._cobj.get_flags, args, kwargs)
+        return self._cobj.get_flags(*args, **kwargs)
     def get_range(self, *args, **kwargs):
-        return apply(self._cobj.get_range, args, kwargs)
+        return self._cobj.get_range(*args, **kwargs)
--- a/Lib/bsddb/dbshelve.py
+++ b/Lib/bsddb/dbshelve.py
@@ -1,4 +1,4 @@
-#!/bin/env python
+#!/usr/bin/env python
 #------------------------------------------------------------------------
 #           Copyright (c) 1997-2001 by Total Control Software
 #                         All Rights Reserved
@@ -29,13 +29,51 @@
 
 #------------------------------------------------------------------------
 
-import cPickle
-try:
-    from UserDict import DictMixin
-except ImportError:
-    # DictMixin is new in Python 2.3
-    class DictMixin: pass
-import db
+import sys
+absolute_import = (sys.version_info[0] >= 3)
+if absolute_import :
+    # Because this syntaxis is not valid before Python 2.5
+    exec("from . import db")
+else :
+    import db
+
+if sys.version_info[0] >= 3 :
+    import cPickle  # Will be converted to "pickle" by "2to3"
+else :
+    if sys.version_info < (2, 6) :
+        import cPickle
+    else :
+        # When we drop support for python 2.4
+        # we could use: (in 2.5 we need a __future__ statement)
+        #
+        #    with warnings.catch_warnings():
+        #        warnings.filterwarnings(...)
+        #        ...
+        #
+        # We can not use "with" as is, because it would be invalid syntax
+        # in python 2.4 and (with no __future__) 2.5.
+        # Here we simulate "with" following PEP 343 :
+        import warnings
+        w = warnings.catch_warnings()
+        w.__enter__()
+        try :
+            warnings.filterwarnings('ignore',
+                message='the cPickle module has been removed in Python 3.0',
+                category=DeprecationWarning)
+            import cPickle
+        finally :
+            w.__exit__()
+        del w
+
+HIGHEST_PROTOCOL = cPickle.HIGHEST_PROTOCOL
+def _dumps(object, protocol):
+    return cPickle.dumps(object, protocol=protocol)
+
+if sys.version_info < (2, 6) :
+    from UserDict import DictMixin as MutableMapping
+else :
+    import collections
+    MutableMapping = collections.MutableMapping
 
 #------------------------------------------------------------------------
 
@@ -78,13 +116,17 @@
 class DBShelveError(db.DBError): pass
 
 
-class DBShelf(DictMixin):
+class DBShelf(MutableMapping):
     """A shelf to hold pickled objects, built upon a bsddb DB object.  It
     automatically pickles/unpickles data objects going to/from the DB.
     """
     def __init__(self, dbenv=None):
         self.db = db.DB(dbenv)
-        self.binary = 1
+        self._closed = True
+        if HIGHEST_PROTOCOL:
+            self.protocol = HIGHEST_PROTOCOL
+        else:
+            self.protocol = 1
 
 
     def __del__(self):
@@ -111,7 +153,7 @@
 
 
     def __setitem__(self, key, value):
-        data = cPickle.dumps(value, self.binary)
+        data = _dumps(value, self.protocol)
         self.db[key] = data
 
 
@@ -120,14 +162,42 @@
 
 
     def keys(self, txn=None):
-        if txn != None:
+        if txn is not None:
             return self.db.keys(txn)
         else:
             return self.db.keys()
 
+    if sys.version_info >= (2, 6) :
+        def __iter__(self) :  # XXX: Load all keys in memory :-(
+            for k in self.db.keys() :
+                yield k
+
+        # Do this when "DB"  support iteration
+        # Or is it enough to pass thru "getattr"?
+        #
+        # def __iter__(self) :
+        #    return self.db.__iter__()
+
+
+    def open(self, *args, **kwargs):
+        self.db.open(*args, **kwargs)
+        self._closed = False
+
+
+    def close(self, *args, **kwargs):
+        self.db.close(*args, **kwargs)
+        self._closed = True
+
+
+    def __repr__(self):
+        if self._closed:
+            return '<DBShelf @ 0x%x - closed>' % (id(self))
+        else:
+            return repr(dict(self.iteritems()))
+
 
     def items(self, txn=None):
-        if txn != None:
+        if txn is not None:
             items = self.db.items(txn)
         else:
             items = self.db.items()
@@ -138,7 +208,7 @@
         return newitems
 
     def values(self, txn=None):
-        if txn != None:
+        if txn is not None:
             values = self.db.values(txn)
         else:
             values = self.db.values()
@@ -149,7 +219,7 @@
     # Other methods
 
     def __append(self, value, txn=None):
-        data = cPickle.dumps(value, self.binary)
+        data = _dumps(value, self.protocol)
         return self.db.append(data, txn)
 
     def append(self, value, txn=None):
@@ -160,8 +230,13 @@
 
     def associate(self, secondaryDB, callback, flags=0):
         def _shelf_callback(priKey, priData, realCallback=callback):
-            data = cPickle.loads(priData)
+            # Safe in Python 2.x because expresion short circuit
+            if sys.version_info[0] < 3 or isinstance(priData, bytes) :
+                data = cPickle.loads(priData)
+            else :
+                data = cPickle.loads(bytes(priData, "iso8859-1"))  # 8 bits
             return realCallback(priKey, data)
+
         return self.db.associate(secondaryDB, _shelf_callback, flags)
 
 
@@ -171,27 +246,27 @@
         # given nothing is passed to the extension module.  That way
         # an exception can be raised if set_get_returns_none is turned
         # off.
-        data = apply(self.db.get, args, kw)
+        data = self.db.get(*args, **kw)
         try:
             return cPickle.loads(data)
-        except (TypeError, cPickle.UnpicklingError):
+        except (EOFError, TypeError, cPickle.UnpicklingError):
             return data  # we may be getting the default value, or None,
                          # so it doesn't need unpickled.
 
     def get_both(self, key, value, txn=None, flags=0):
-        data = cPickle.dumps(value, self.binary)
+        data = _dumps(value, self.protocol)
         data = self.db.get(key, data, txn, flags)
         return cPickle.loads(data)
 
 
     def cursor(self, txn=None, flags=0):
         c = DBShelfCursor(self.db.cursor(txn, flags))
-        c.binary = self.binary
+        c.protocol = self.protocol
         return c
 
 
     def put(self, key, value, txn=None, flags=0):
-        data = cPickle.dumps(value, self.binary)
+        data = _dumps(value, self.protocol)
         return self.db.put(key, data, txn, flags)
 
 
@@ -227,18 +302,20 @@
     #----------------------------------------------
 
     def dup(self, flags=0):
-        return DBShelfCursor(self.dbc.dup(flags))
+        c = DBShelfCursor(self.dbc.dup(flags))
+        c.protocol = self.protocol
+        return c
 
 
     def put(self, key, value, flags=0):
-        data = cPickle.dumps(value, self.binary)
+        data = _dumps(value, self.protocol)
         return self.dbc.put(key, data, flags)
 
 
     def get(self, *args):
         count = len(args)  # a method overloading hack
         method = getattr(self, 'get_%d' % count)
-        apply(method, args)
+        method(*args)
 
     def get_1(self, flags):
         rec = self.dbc.get(flags)
@@ -249,7 +326,7 @@
         return self._extract(rec)
 
     def get_3(self, key, value, flags):
-        data = cPickle.dumps(value, self.binary)
+        data = _dumps(value, self.protocol)
         rec = self.dbc.get(key, flags)
         return self._extract(rec)
 
@@ -266,7 +343,7 @@
 
 
     def get_both(self, key, value, flags=0):
-        data = cPickle.dumps(value, self.binary)
+        data = _dumps(value, self.protocol)
         rec = self.dbc.get_both(key, flags)
         return self._extract(rec)
 
@@ -290,7 +367,11 @@
             return None
         else:
             key, data = rec
-            return key, cPickle.loads(data)
+            # Safe in Python 2.x because expresion short circuit
+            if sys.version_info[0] < 3 or isinstance(data, bytes) :
+                return key, cPickle.loads(data)
+            else :
+                return key, cPickle.loads(bytes(data, "iso8859-1"))  # 8 bits
 
     #----------------------------------------------
     # Methods allowed to pass-through to self.dbc
--- a/Lib/bsddb/dbtables.py
+++ b/Lib/bsddb/dbtables.py
@@ -10,34 +10,54 @@
 #               software has been tested, but no warranty is expressed or
 #               implied.
 #
-#   --  Gregory P. Smith <greg@electricrain.com>
+#   --  Gregory P. Smith <greg@krypto.org>
 
 # This provides a simple database table interface built on top of
-# the Python BerkeleyDB 3 interface.
+# the Python Berkeley DB 3 interface.
 #
-_cvsid = '$Id: dbtables.py 58760 2007-11-01 21:22:40Z gregory.p.smith $'
+_cvsid = '$Id$'
 
 import re
 import sys
 import copy
-import struct
 import random
-from types import ListType, StringType
-import cPickle as pickle
+import struct
+
+
+if sys.version_info[0] >= 3 :
+    import pickle
+else :
+    if sys.version_info < (2, 6) :
+        import cPickle as pickle
+    else :
+        # When we drop support for python 2.4
+        # we could use: (in 2.5 we need a __future__ statement)
+        #
+        #    with warnings.catch_warnings():
+        #        warnings.filterwarnings(...)
+        #        ...
+        #
+        # We can not use "with" as is, because it would be invalid syntax
+        # in python 2.4 and (with no __future__) 2.5.
+        # Here we simulate "with" following PEP 343 :
+        import warnings
+        w = warnings.catch_warnings()
+        w.__enter__()
+        try :
+            warnings.filterwarnings('ignore',
+                message='the cPickle module has been removed in Python 3.0',
+                category=DeprecationWarning)
+            import cPickle as pickle
+        finally :
+            w.__exit__()
+        del w
 
 try:
     # For Pythons w/distutils pybsddb
-    from bsddb3.db import *
+    from bsddb3 import db
 except ImportError:
     # For Python 2.3
-    from bsddb.db import *
-
-# XXX(nnorwitz): is this correct? DBIncompleteError is conditional in _bsddb.c
-try:
-    DBIncompleteError
-except NameError:
-    class DBIncompleteError(Exception):
-        pass
+    from bsddb import db
 
 class TableDBError(StandardError):
     pass
@@ -105,6 +125,7 @@
                      # row in the table.  (no data is stored)
 _rowid_str_len = 8   # length in bytes of the unique rowid strings
 
+
 def _data_key(table, col, rowid):
     return table + _data + col + _data + rowid
 
@@ -139,41 +160,108 @@
                  recover=0, dbflags=0):
         """bsdTableDB(filename, dbhome, create=0, truncate=0, mode=0600)
 
-        Open database name in the dbhome BerkeleyDB directory.
+        Open database name in the dbhome Berkeley DB directory.
         Use keyword arguments when calling this constructor.
         """
         self.db = None
-        myflags = DB_THREAD
+        myflags = db.DB_THREAD
         if create:
-            myflags |= DB_CREATE
-        flagsforenv = (DB_INIT_MPOOL | DB_INIT_LOCK | DB_INIT_LOG |
-                       DB_INIT_TXN | dbflags)
+            myflags |= db.DB_CREATE
+        flagsforenv = (db.DB_INIT_MPOOL | db.DB_INIT_LOCK | db.DB_INIT_LOG |
+                       db.DB_INIT_TXN | dbflags)
         # DB_AUTO_COMMIT isn't a valid flag for env.open()
         try:
-            dbflags |= DB_AUTO_COMMIT
+            dbflags |= db.DB_AUTO_COMMIT
         except AttributeError:
             pass
         if recover:
-            flagsforenv = flagsforenv | DB_RECOVER
-        self.env = DBEnv()
+            flagsforenv = flagsforenv | db.DB_RECOVER
+        self.env = db.DBEnv()
         # enable auto deadlock avoidance
-        self.env.set_lk_detect(DB_LOCK_DEFAULT)
+        self.env.set_lk_detect(db.DB_LOCK_DEFAULT)
         self.env.open(dbhome, myflags | flagsforenv)
         if truncate:
-            myflags |= DB_TRUNCATE
-        self.db = DB(self.env)
+            myflags |= db.DB_TRUNCATE
+        self.db = db.DB(self.env)
         # this code relies on DBCursor.set* methods to raise exceptions
         # rather than returning None
         self.db.set_get_returns_none(1)
         # allow duplicate entries [warning: be careful w/ metadata]
-        self.db.set_flags(DB_DUP)
-        self.db.open(filename, DB_BTREE, dbflags | myflags, mode)
+        self.db.set_flags(db.DB_DUP)
+        self.db.open(filename, db.DB_BTREE, dbflags | myflags, mode)
         self.dbfilename = filename
+
+        if sys.version_info[0] >= 3 :
+            class cursor_py3k(object) :
+                def __init__(self, dbcursor) :
+                    self._dbcursor = dbcursor
+
+                def close(self) :
+                    return self._dbcursor.close()
+
+                def set_range(self, search) :
+                    v = self._dbcursor.set_range(bytes(search, "iso8859-1"))
+                    if v is not None :
+                        v = (v[0].decode("iso8859-1"),
+                                v[1].decode("iso8859-1"))
+                    return v
+
+                def __next__(self) :
+                    v = getattr(self._dbcursor, "next")()
+                    if v is not None :
+                        v = (v[0].decode("iso8859-1"),
+                                v[1].decode("iso8859-1"))
+                    return v
+
+            class db_py3k(object) :
+                def __init__(self, db) :
+                    self._db = db
+
+                def cursor(self, txn=None) :
+                    return cursor_py3k(self._db.cursor(txn=txn))
+
+                def has_key(self, key, txn=None) :
+                    return getattr(self._db,"has_key")(bytes(key, "iso8859-1"),
+                            txn=txn)
+
+                def put(self, key, value, flags=0, txn=None) :
+                    key = bytes(key, "iso8859-1")
+                    if value is not None :
+                        value = bytes(value, "iso8859-1")
+                    return self._db.put(key, value, flags=flags, txn=txn)
+
+                def put_bytes(self, key, value, txn=None) :
+                    key = bytes(key, "iso8859-1")
+                    return self._db.put(key, value, txn=txn)
+
+                def get(self, key, txn=None, flags=0) :
+                    key = bytes(key, "iso8859-1")
+                    v = self._db.get(key, txn=txn, flags=flags)
+                    if v is not None :
+                        v = v.decode("iso8859-1")
+                    return v
+
+                def get_bytes(self, key, txn=None, flags=0) :
+                    key = bytes(key, "iso8859-1")
+                    return self._db.get(key, txn=txn, flags=flags)
+
+                def delete(self, key, txn=None) :
+                    key = bytes(key, "iso8859-1")
+                    return self._db.delete(key, txn=txn)
+
+                def close (self) :
+                    return self._db.close()
+
+            self.db = db_py3k(self.db)
+        else :  # Python 2.x
+            pass
+
         # Initialize the table names list if this is a new database
         txn = self.env.txn_begin()
         try:
-            if not self.db.has_key(_table_names_key, txn):
-                self.db.put(_table_names_key, pickle.dumps([], 1), txn=txn)
+            if not getattr(self.db, "has_key")(_table_names_key, txn):
+                getattr(self.db, "put_bytes", self.db.put) \
+                        (_table_names_key, pickle.dumps([], 1), txn=txn)
         # Yes, bare except
         except:
             txn.abort()
@@ -195,16 +283,10 @@
             self.env = None
 
     def checkpoint(self, mins=0):
-        try:
-            self.env.txn_checkpoint(mins)
-        except DBIncompleteError:
-            pass
+        self.env.txn_checkpoint(mins)
 
     def sync(self):
-        try:
-            self.db.sync()
-        except DBIncompleteError:
-            pass
+        self.db.sync()
 
     def _db_print(self) :
         """Print the database to stdout for debugging"""
@@ -220,7 +302,7 @@
                 else:
                     cur.close()
                     return
-        except DBNotFoundError:
+        except db.DBNotFoundError:
             cur.close()
 
 
@@ -229,7 +311,8 @@
 
         raises TableDBError if it already exists or for other DB errors.
         """
-        assert isinstance(columns, ListType)
+        assert isinstance(columns, list)
+
         txn = None
         try:
             # checking sanity of the table and column names here on
@@ -243,41 +326,47 @@
                         "bad column name: contains reserved metastrings")
 
             columnlist_key = _columns_key(table)
-            if self.db.has_key(columnlist_key):
+            if getattr(self.db, "has_key")(columnlist_key):
                 raise TableAlreadyExists, "table already exists"
 
             txn = self.env.txn_begin()
             # store the table's column info
-            self.db.put(columnlist_key, pickle.dumps(columns, 1), txn=txn)
+            getattr(self.db, "put_bytes", self.db.put)(columnlist_key,
+                    pickle.dumps(columns, 1), txn=txn)
 
             # add the table name to the tablelist
-            tablelist = pickle.loads(self.db.get(_table_names_key, txn=txn,
-                                                 flags=DB_RMW))
+            tablelist = pickle.loads(getattr(self.db, "get_bytes",
+                self.db.get) (_table_names_key, txn=txn, flags=db.DB_RMW))
             tablelist.append(table)
             # delete 1st, in case we opened with DB_DUP
-            self.db.delete(_table_names_key, txn)
-            self.db.put(_table_names_key, pickle.dumps(tablelist, 1), txn=txn)
+            self.db.delete(_table_names_key, txn=txn)
+            getattr(self.db, "put_bytes", self.db.put)(_table_names_key,
+                    pickle.dumps(tablelist, 1), txn=txn)
 
             txn.commit()
             txn = None
-        except DBError, dberror:
+        except db.DBError, dberror:
             if txn:
                 txn.abort()
-            raise TableDBError, dberror[1]
+            if sys.version_info < (2, 6) :
+                raise TableDBError, dberror[1]
+            else :
+                raise TableDBError, dberror.args[1]
 
 
     def ListTableColumns(self, table):
         """Return a list of columns in the given table.
         [] if the table doesn't exist.
         """
-        assert isinstance(table, StringType)
+        assert isinstance(table, str)
         if contains_metastrings(table):
             raise ValueError, "bad table name: contains reserved metastrings"
 
         columnlist_key = _columns_key(table)
-        if not self.db.has_key(columnlist_key):
+        if not getattr(self.db, "has_key")(columnlist_key):
             return []
-        pickledcolumnlist = self.db.get(columnlist_key)
+        pickledcolumnlist = getattr(self.db, "get_bytes",
+                self.db.get)(columnlist_key)
         if pickledcolumnlist:
             return pickle.loads(pickledcolumnlist)
         else:
@@ -285,7 +374,7 @@
 
     def ListTables(self):
         """Return a list of tables in this database."""
-        pickledtablelist = self.db.get(_table_names_key)
+        pickledtablelist = self.db.get_get(_table_names_key)
         if pickledtablelist:
             return pickle.loads(pickledtablelist)
         else:
@@ -300,7 +389,8 @@
         additional columns present in the given list as well as
         all of its current columns.
         """
-        assert isinstance(columns, ListType)
+        assert isinstance(columns, list)
+
         try:
             self.CreateTable(table, columns)
         except TableAlreadyExists:
@@ -312,7 +402,8 @@
 
                 # load the current column list
                 oldcolumnlist = pickle.loads(
-                    self.db.get(columnlist_key, txn=txn, flags=DB_RMW))
+                    getattr(self.db, "get_bytes",
+                        self.db.get)(columnlist_key, txn=txn, flags=db.DB_RMW))
                 # create a hash table for fast lookups of column names in the
                 # loop below
                 oldcolumnhash = {}
@@ -323,14 +414,14 @@
                 # column names
                 newcolumnlist = copy.copy(oldcolumnlist)
                 for c in columns:
-                    if not oldcolumnhash.has_key(c):
+                    if not c in oldcolumnhash:
                         newcolumnlist.append(c)
 
                 # store the table's new extended column list
                 if newcolumnlist != oldcolumnlist :
                     # delete the old one first since we opened with DB_DUP
-                    self.db.delete(columnlist_key, txn)
-                    self.db.put(columnlist_key,
+                    self.db.delete(columnlist_key, txn=txn)
+                    getattr(self.db, "put_bytes", self.db.put)(columnlist_key,
                                 pickle.dumps(newcolumnlist, 1),
                                 txn=txn)
 
@@ -338,18 +429,22 @@
                 txn = None
 
                 self.__load_column_info(table)
-            except DBError, dberror:
+            except db.DBError, dberror:
                 if txn:
                     txn.abort()
-                raise TableDBError, dberror[1]
+                if sys.version_info < (2, 6) :
+                    raise TableDBError, dberror[1]
+                else :
+                    raise TableDBError, dberror.args[1]
 
 
     def __load_column_info(self, table) :
         """initialize the self.__tablecolumns dict"""
         # check the column names
         try:
-            tcolpickles = self.db.get(_columns_key(table))
-        except DBNotFoundError:
+            tcolpickles = getattr(self.db, "get_bytes",
+                    self.db.get)(_columns_key(table))
+        except db.DBNotFoundError:
             raise TableDBError, "unknown table: %r" % (table,)
         if not tcolpickles:
             raise TableDBError, "unknown table: %r" % (table,)
@@ -367,11 +462,14 @@
                 blist.append(random.randint(0,255))
             newid = struct.pack('B'*_rowid_str_len, *blist)
 
+            if sys.version_info[0] >= 3 :
+                newid = newid.decode("iso8859-1")  # 8 bits
+
             # Guarantee uniqueness by adding this key to the database
             try:
                 self.db.put(_rowid_key(table, newid), None, txn=txn,
-                            flags=DB_NOOVERWRITE)
-            except DBKeyExistError:
+                            flags=db.DB_NOOVERWRITE)
+            except db.DBKeyExistError:
                 pass
             else:
                 unique = 1
@@ -383,13 +481,14 @@
         """Insert(table, datadict) - Insert a new row into the table
         using the keys+values from rowdict as the column values.
         """
+
         txn = None
         try:
-            if not self.db.has_key(_columns_key(table)):
+            if not getattr(self.db, "has_key")(_columns_key(table)):
                 raise TableDBError, "unknown table"
 
             # check the validity of each column name
-            if not self.__tablecolumns.has_key(table):
+            if not table in self.__tablecolumns:
                 self.__load_column_info(table)
             for column in rowdict.keys() :
                 if not self.__tablecolumns[table].count(column):
@@ -407,7 +506,7 @@
             txn.commit()
             txn = None
 
-        except DBError, dberror:
+        except db.DBError, dberror:
             # WIBNI we could just abort the txn and re-raise the exception?
             # But no, because TableDBError is not related to DBError via
             # inheritance, so it would be backwards incompatible.  Do the next
@@ -416,7 +515,10 @@
             if txn:
                 txn.abort()
                 self.db.delete(_rowid_key(table, rowid))
-            raise TableDBError, dberror[1], info[2]
+            if sys.version_info < (2, 6) :
+                raise TableDBError, dberror[1], info[2]
+            else :
+                raise TableDBError, dberror.args[1], info[2]
 
 
     def Modify(self, table, conditions={}, mappings={}):
@@ -430,6 +532,7 @@
           condition callable expecting the data string as an argument and
           returning the new string for that column.
         """
+
         try:
             matching_rowids = self.__Select(table, [], conditions)
 
@@ -447,13 +550,13 @@
                                 txn=txn)
                             self.db.delete(
                                 _data_key(table, column, rowid),
-                                txn)
-                        except DBNotFoundError:
+                                txn=txn)
+                        except db.DBNotFoundError:
                              # XXXXXXX row key somehow didn't exist, assume no
                              # error
                             dataitem = None
                         dataitem = mappings[column](dataitem)
-                        if dataitem <> None:
+                        if dataitem is not None:
                             self.db.put(
                                 _data_key(table, column, rowid),
                                 dataitem, txn=txn)
@@ -466,8 +569,11 @@
                         txn.abort()
                     raise
 
-        except DBError, dberror:
-            raise TableDBError, dberror[1]
+        except db.DBError, dberror:
+            if sys.version_info < (2, 6) :
+                raise TableDBError, dberror[1]
+            else :
+                raise TableDBError, dberror.args[1]
 
     def Delete(self, table, conditions={}):
         """Delete(table, conditions) - Delete items matching the given
@@ -477,6 +583,7 @@
           condition functions expecting the data string as an
           argument and returning a boolean.
         """
+
         try:
             matching_rowids = self.__Select(table, [], conditions)
 
@@ -490,24 +597,27 @@
                         # delete the data key
                         try:
                             self.db.delete(_data_key(table, column, rowid),
-                                           txn)
-                        except DBNotFoundError:
+                                           txn=txn)
+                        except db.DBNotFoundError:
                             # XXXXXXX column may not exist, assume no error
                             pass
 
                     try:
-                        self.db.delete(_rowid_key(table, rowid), txn)
-                    except DBNotFoundError:
+                        self.db.delete(_rowid_key(table, rowid), txn=txn)
+                    except db.DBNotFoundError:
                         # XXXXXXX row key somehow didn't exist, assume no error
                         pass
                     txn.commit()
                     txn = None
-                except DBError, dberror:
+                except db.DBError, dberror:
                     if txn:
                         txn.abort()
                     raise
-        except DBError, dberror:
-            raise TableDBError, dberror[1]
+        except db.DBError, dberror:
+            if sys.version_info < (2, 6) :
+                raise TableDBError, dberror[1]
+            else :
+                raise TableDBError, dberror.args[1]
 
 
     def Select(self, table, columns, conditions={}):
@@ -521,13 +631,16 @@
           argument and returning a boolean.
         """
         try:
-            if not self.__tablecolumns.has_key(table):
+            if not table in self.__tablecolumns:
                 self.__load_column_info(table)
             if columns is None:
                 columns = self.__tablecolumns[table]
             matching_rowids = self.__Select(table, columns, conditions)
-        except DBError, dberror:
-            raise TableDBError, dberror[1]
+        except db.DBError, dberror:
+            if sys.version_info < (2, 6) :
+                raise TableDBError, dberror[1]
+            else :
+                raise TableDBError, dberror.args[1]
         # return the matches as a list of dictionaries
         return matching_rowids.values()
 
@@ -542,7 +655,7 @@
         argument and returning a boolean.
         """
         # check the validity of each column name
-        if not self.__tablecolumns.has_key(table):
+        if not table in self.__tablecolumns:
             self.__load_column_info(table)
         if columns is None:
             columns = self.tablecolumns[table]
@@ -562,6 +675,13 @@
             a = atuple[1]
             b = btuple[1]
             if type(a) is type(b):
+
+                # Needed for python 3. "cmp" vanished in 3.0.1
+                def cmp(a, b) :
+                    if a==b : return 0
+                    if a<b : return -1
+                    return 1
+
                 if isinstance(a, PrefixCond) and isinstance(b, PrefixCond):
                     # longest prefix first
                     return cmp(len(b.prefix), len(a.prefix))
@@ -580,8 +700,19 @@
             # leave all unknown condition callables alone as equals
             return 0
 
-        conditionlist = conditions.items()
-        conditionlist.sort(cmp_conditions)
+        if sys.version_info < (2, 6) :
+            conditionlist = conditions.items()
+            conditionlist.sort(cmp_conditions)
+        else :  # Insertion Sort. Please, improve
+            conditionlist = []
+            for i in conditions.items() :
+                for j, k in enumerate(conditionlist) :
+                    r = cmp_conditions(k, i)
+                    if r == 1 :
+                        conditionlist.insert(j, i)
+                        break
+                else :
+                    conditionlist.append(i)
 
         # Apply conditions to column data to find what we want
         cur = self.db.cursor()
@@ -601,23 +732,23 @@
                     # extract the rowid from the key
                     rowid = key[-_rowid_str_len:]
 
-                    if not rejected_rowids.has_key(rowid):
+                    if not rowid in rejected_rowids:
                         # if no condition was specified or the condition
                         # succeeds, add row to our match list.
                         if not condition or condition(data):
-                            if not matching_rowids.has_key(rowid):
+                            if not rowid in matching_rowids:
                                 matching_rowids[rowid] = {}
                             if savethiscolumndata:
                                 matching_rowids[rowid][column] = data
                         else:
-                            if matching_rowids.has_key(rowid):
+                            if rowid in matching_rowids:
                                 del matching_rowids[rowid]
                             rejected_rowids[rowid] = rowid
 
                     key, data = cur.next()
 
-            except DBError, dberror:
-                if dberror[0] != DB_NOTFOUND:
+            except db.DBError, dberror:
+                if dberror.args[0] != db.DB_NOTFOUND:
                     raise
                 continue
 
@@ -631,14 +762,18 @@
         if len(columns) > 0:
             for rowid, rowdata in matching_rowids.items():
                 for column in columns:
-                    if rowdata.has_key(column):
+                    if column in rowdata:
                         continue
                     try:
                         rowdata[column] = self.db.get(
                             _data_key(table, column, rowid))
-                    except DBError, dberror:
-                        if dberror[0] != DB_NOTFOUND:
-                            raise
+                    except db.DBError, dberror:
+                        if sys.version_info < (2, 6) :
+                            if dberror[0] != db.DB_NOTFOUND:
+                                raise
+                        else :
+                            if dberror.args[0] != db.DB_NOTFOUND:
+                                raise
                         rowdata[column] = None
 
         # return the matches
@@ -652,7 +787,7 @@
             txn = self.env.txn_begin()
 
             # delete the column list
-            self.db.delete(_columns_key(table), txn)
+            self.db.delete(_columns_key(table), txn=txn)
 
             cur = self.db.cursor(txn)
 
@@ -661,7 +796,7 @@
             while 1:
                 try:
                     key, data = cur.set_range(table_key)
-                except DBNotFoundError:
+                except db.DBNotFoundError:
                     break
                 # only delete items in this table
                 if key[:len(table_key)] != table_key:
@@ -673,7 +808,7 @@
             while 1:
                 try:
                     key, data = cur.set_range(table_key)
-                except DBNotFoundError:
+                except db.DBNotFoundError:
                     break
                 # only delete items in this table
                 if key[:len(table_key)] != table_key:
@@ -684,23 +819,25 @@
 
             # delete the tablename from the table name list
             tablelist = pickle.loads(
-                self.db.get(_table_names_key, txn=txn, flags=DB_RMW))
+                getattr(self.db, "get_bytes", self.db.get)(_table_names_key,
+                    txn=txn, flags=db.DB_RMW))
             try:
                 tablelist.remove(table)
             except ValueError:
                 # hmm, it wasn't there, oh well, that's what we want.
                 pass
             # delete 1st, incase we opened with DB_DUP
-            self.db.delete(_table_names_key, txn)
-            self.db.put(_table_names_key, pickle.dumps(tablelist, 1), txn=txn)
+            self.db.delete(_table_names_key, txn=txn)
+            getattr(self.db, "put_bytes", self.db.put)(_table_names_key,
+                    pickle.dumps(tablelist, 1), txn=txn)
 
             txn.commit()
             txn = None
 
-            if self.__tablecolumns.has_key(table):
+            if table in self.__tablecolumns:
                 del self.__tablecolumns[table]
 
-        except DBError, dberror:
+        except db.DBError, dberror:
             if txn:
                 txn.abort()
-            raise TableDBError, dberror[1]
+            raise TableDBError(dberror.args[1])
--- a/Lib/bsddb/dbutils.py
+++ b/Lib/bsddb/dbutils.py
@@ -9,7 +9,7 @@
 #               software has been tested, but no warranty is expressed or
 #               implied.
 #
-# Author: Gregory P. Smith <greg@electricrain.com>
+# Author: Gregory P. Smith <greg@krypto.org>
 #
 # Note: I don't know how useful this is in reality since when a
 #       DBLockDeadlockError happens the current transaction is supposed to be
@@ -26,7 +26,13 @@
 #
 from time import sleep as _sleep
 
-import db
+import sys
+absolute_import = (sys.version_info[0] >= 3)
+if absolute_import :
+    # Because this syntaxis is not valid before Python 2.5
+    exec("from . import db")
+else :
+    import db
 
 # always sleep at least N seconds between retrys
 _deadlock_MinSleepTime = 1.0/128
@@ -55,7 +61,7 @@
     """
     sleeptime = _deadlock_MinSleepTime
     max_retries = _kwargs.get('max_retries', -1)
-    if _kwargs.has_key('max_retries'):
+    if 'max_retries' in _kwargs:
         del _kwargs['max_retries']
     while True:
         try:
--- a/Modules/_bsddb.c
+++ b/Modules/_bsddb.c
@@ -36,19 +36,22 @@
 /*
  * Handwritten code to wrap version 3.x of the Berkeley DB library,
  * written to replace a SWIG-generated file.  It has since been updated
- * to compile with BerkeleyDB versions 3.2 through 4.2.
+ * to compile with Berkeley DB versions 3.2 through 4.2.
  *
  * This module was started by Andrew Kuchling to remove the dependency
- * on SWIG in a package by Gregory P. Smith <greg@electricrain.com> who
- * based his work on a similar package by Robin Dunn <robin@alldunn.com>
- * which wrapped Berkeley DB 2.7.x.
+ * on SWIG in a package by Gregory P. Smith who based his work on a
+ * similar package by Robin Dunn <robin@alldunn.com> which wrapped
+ * Berkeley DB 2.7.x.
  *
  * Development of this module then returned full circle back to Robin Dunn
  * who worked on behalf of Digital Creations to complete the wrapping of
  * the DB 3.x API and to build a solid unit test suite.  Robin has
  * since gone onto other projects (wxPython).
  *
- * Gregory P. Smith <greg@electricrain.com> is once again the maintainer.
+ * Gregory P. Smith <greg@krypto.org> was once again the maintainer.
+ *
+ * Since January 2008, new maintainer is Jesus Cea <jcea@jcea.es>.
+ * Jesus Cea licenses this code to PSF under a Contributor Agreement.
  *
  * Use the pybsddb-users@lists.sf.net mailing list for all questions.
  * Things can change faster than the header of this file is updated.  This
@@ -61,7 +64,7 @@
  *
  * http://www.python.org/peps/pep-0291.html
  *
- * This module contains 6 types:
+ * This module contains 7 types:
  *
  * DB           (Database)
  * DBCursor     (Database Cursor)
@@ -69,6 +72,11 @@
  * DBTxn        (An explicit database transaction)
  * DBLock       (A lock handle)
  * DBSequence   (Sequence)
+ * DBSite       (Site)
+ *
+ * More datatypes added:
+ *
+ * DBLogCursor  (Log Cursor)
  *
  */
 
@@ -87,59 +95,52 @@
 
 #include <stddef.h>   /* for offsetof() */
 #include <Python.h>
-#include <db.h>
-
-/* --------------------------------------------------------------------- */
-/* Various macro definitions */
 
-/* 40 = 4.0, 33 = 3.3; this will break if the second number is > 9 */
-#define DBVER (DB_VERSION_MAJOR * 10 + DB_VERSION_MINOR)
-#if DB_VERSION_MINOR > 9
-#error "eek! DBVER can't handle minor versions > 9"
-#endif
+#define COMPILING_BSDDB_C
+#include "bsddb.h"
+#undef COMPILING_BSDDB_C
 
-#define PY_BSDDB_VERSION "4.4.5.3"
-static char *rcs_id = "$Id: _bsddb.c 63404 2008-05-17 06:46:39Z gregory.p.smith $";
+static char *rcs_id = "$Id$";
 
+/* --------------------------------------------------------------------- */
+/* Various macro definitions */
 
 #if (PY_VERSION_HEX < 0x02050000)
 typedef int Py_ssize_t;
 #endif
 
+#if (PY_VERSION_HEX < 0x02060000)  /* really: before python trunk r63675 */
+/* This code now uses PyBytes* API function names instead of PyString*.
+ * These #defines map to their equivalent on earlier python versions.    */
+#define PyBytes_FromStringAndSize PyString_FromStringAndSize
+#define PyBytes_FromString PyString_FromString
+#define PyBytes_AsStringAndSize PyString_AsStringAndSize
+#define PyBytes_Check PyString_Check
+#define PyBytes_GET_SIZE PyString_GET_SIZE
+#define PyBytes_AS_STRING PyString_AS_STRING
+#endif
+
+#if (PY_VERSION_HEX >= 0x03000000)
+#define NUMBER_Check    PyLong_Check
+#define NUMBER_AsLong   PyLong_AsLong
+#define NUMBER_FromLong PyLong_FromLong
+#else
+#define NUMBER_Check    PyInt_Check
+#define NUMBER_AsLong   PyInt_AsLong
+#define NUMBER_FromLong PyInt_FromLong
+#endif
+
 #ifdef WITH_THREAD
 
 /* These are for when calling Python --> C */
 #define MYDB_BEGIN_ALLOW_THREADS Py_BEGIN_ALLOW_THREADS;
 #define MYDB_END_ALLOW_THREADS Py_END_ALLOW_THREADS;
 
-/* For 2.3, use the PyGILState_ calls */
-#if (PY_VERSION_HEX >= 0x02030000)
-#define MYDB_USE_GILSTATE
-#endif
-
 /* and these are for calling C --> Python */
-#if defined(MYDB_USE_GILSTATE)
 #define MYDB_BEGIN_BLOCK_THREADS \
-		PyGILState_STATE __savestate = PyGILState_Ensure();
+                PyGILState_STATE __savestate = PyGILState_Ensure();
 #define MYDB_END_BLOCK_THREADS \
-		PyGILState_Release(__savestate);
-#else /* MYDB_USE_GILSTATE */
-/* Pre GILState API - do it the long old way */
-static PyInterpreterState* _db_interpreterState = NULL;
-#define MYDB_BEGIN_BLOCK_THREADS {                              \
-        PyThreadState* prevState;                               \
-        PyThreadState* newState;                                \
-        PyEval_AcquireLock();                                   \
-        newState  = PyThreadState_New(_db_interpreterState);    \
-        prevState = PyThreadState_Swap(newState);
-
-#define MYDB_END_BLOCK_THREADS                                  \
-        newState = PyThreadState_Swap(prevState);               \
-        PyThreadState_Clear(newState);                          \
-        PyEval_ReleaseLock();                                   \
-        PyThreadState_Delete(newState);                         \
-        }
-#endif /* MYDB_USE_GILSTATE */
+                PyGILState_Release(__savestate);
 
 #else
 /* Compiled without threads - avoid all this cruft */
@@ -150,9 +151,6 @@
 
 #endif
 
-/* Should DB_INCOMPLETE be turned into a warning or an exception? */
-#define INCOMPLETE_IS_WARNING 1
-
 /* --------------------------------------------------------------------- */
 /* Exceptions */
 
@@ -167,152 +165,159 @@
 static PyObject* DBRunRecoveryError;    /* DB_RUNRECOVERY */
 static PyObject* DBVerifyBadError;      /* DB_VERIFY_BAD */
 static PyObject* DBNoServerError;       /* DB_NOSERVER */
+#if (DBVER < 52)
 static PyObject* DBNoServerHomeError;   /* DB_NOSERVER_HOME */
 static PyObject* DBNoServerIDError;     /* DB_NOSERVER_ID */
-#if (DBVER >= 33)
+#endif
 static PyObject* DBPageNotFoundError;   /* DB_PAGE_NOTFOUND */
 static PyObject* DBSecondaryBadError;   /* DB_SECONDARY_BAD */
-#endif
-
-#if !INCOMPLETE_IS_WARNING
-static PyObject* DBIncompleteError;     /* DB_INCOMPLETE */
-#endif
 
 static PyObject* DBInvalidArgError;     /* EINVAL */
 static PyObject* DBAccessError;         /* EACCES */
 static PyObject* DBNoSpaceError;        /* ENOSPC */
-static PyObject* DBNoMemoryError;       /* DB_BUFFER_SMALL (ENOMEM when < 4.3) */
+static PyObject* DBNoMemoryError;       /* DB_BUFFER_SMALL */
 static PyObject* DBAgainError;          /* EAGAIN */
 static PyObject* DBBusyError;           /* EBUSY  */
 static PyObject* DBFileExistsError;     /* EEXIST */
 static PyObject* DBNoSuchFileError;     /* ENOENT */
 static PyObject* DBPermissionsError;    /* EPERM  */
 
-#if (DBVER < 43)
-#define	DB_BUFFER_SMALL		ENOMEM
+static PyObject* DBRepHandleDeadError;  /* DB_REP_HANDLE_DEAD */
+#if (DBVER >= 44)
+static PyObject* DBRepLockoutError;     /* DB_REP_LOCKOUT */
 #endif
 
+#if (DBVER >= 46)
+static PyObject* DBRepLeaseExpiredError; /* DB_REP_LEASE_EXPIRED */
+#endif
 
-/* --------------------------------------------------------------------- */
-/* Structure definitions */
-
-#if PYTHON_API_VERSION >= 1010       /* python >= 2.1 support weak references */
-#define HAVE_WEAKREF
-#else
-#undef HAVE_WEAKREF
+#if (DBVER >= 47)
+static PyObject* DBForeignConflictError; /* DB_FOREIGN_CONFLICT */
 #endif
 
-/* if Python >= 2.1 better support warnings */
-#if PYTHON_API_VERSION >= 1010
-#define HAVE_WARNINGS
-#else
-#undef HAVE_WARNINGS
+
+static PyObject* DBRepUnavailError;     /* DB_REP_UNAVAIL */
+
+#if (DBVER < 48)
+#define DB_GID_SIZE DB_XIDDATASIZE
 #endif
 
-#if PYTHON_API_VERSION <= 1007
-    /* 1.5 compatibility */
-#define PyObject_New PyObject_NEW
-#define PyObject_Del PyMem_DEL
+
+/* --------------------------------------------------------------------- */
+/* Structure definitions */
+
+#if PYTHON_API_VERSION < 1010
+#error "Python 2.1 or later required"
 #endif
 
-struct behaviourFlags {
-    /* What is the default behaviour when DB->get or DBCursor->get returns a
-       DB_NOTFOUND || DB_KEYEMPTY error?  Return None or raise an exception? */
-    unsigned int getReturnsNone : 1;
-    /* What is the default behaviour for DBCursor.set* methods when DBCursor->get
-     * returns a DB_NOTFOUND || DB_KEYEMPTY  error?  Return None or raise? */
-    unsigned int cursorSetReturnsNone : 1;
-};
 
+/* Defaults for moduleFlags in DBEnvObject and DBObject. */
 #define DEFAULT_GET_RETURNS_NONE                1
 #define DEFAULT_CURSOR_SET_RETURNS_NONE         1   /* 0 in pybsddb < 4.2, python < 2.4 */
 
-typedef struct {
-    PyObject_HEAD
-    DB_ENV*     db_env;
-    u_int32_t   flags;             /* saved flags from open() */
-    int         closed;
-    struct behaviourFlags moduleFlags;
-#ifdef HAVE_WEAKREF
-    PyObject        *in_weakreflist; /* List of weak references */
-#endif
-} DBEnvObject;
-
-
-typedef struct {
-    PyObject_HEAD
-    DB*             db;
-    DBEnvObject*    myenvobj;  /* PyObject containing the DB_ENV */
-    u_int32_t       flags;     /* saved flags from open() */
-    u_int32_t       setflags;  /* saved flags from set_flags() */
-    int             haveStat;
-    struct behaviourFlags moduleFlags;
-#if (DBVER >= 33)
-    PyObject*       associateCallback;
-    PyObject*       btCompareCallback;
-    int             primaryDBType;
-#endif
-#ifdef HAVE_WEAKREF
-    PyObject        *in_weakreflist; /* List of weak references */
-#endif
-} DBObject;
-
-
-typedef struct {
-    PyObject_HEAD
-    DBC*            dbc;
-    DBObject*       mydb;
-#ifdef HAVE_WEAKREF
-    PyObject        *in_weakreflist; /* List of weak references */
-#endif
-} DBCursorObject;
-
-
-typedef struct {
-    PyObject_HEAD
-    DB_TXN*         txn;
-    PyObject        *env;
-#ifdef HAVE_WEAKREF
-    PyObject        *in_weakreflist; /* List of weak references */
-#endif
-} DBTxnObject;
-
-
-typedef struct {
-    PyObject_HEAD
-    DB_LOCK         lock;
-#ifdef HAVE_WEAKREF
-    PyObject        *in_weakreflist; /* List of weak references */
-#endif
-} DBLockObject;
-
-#if (DBVER >= 43)
-typedef struct {
-    PyObject_HEAD
-    DB_SEQUENCE*     sequence;
-    DBObject*        mydb;
-#ifdef HAVE_WEAKREF
-    PyObject        *in_weakreflist; /* List of weak references */
+
+/* See comment in Python 2.6 "object.h" */
+#ifndef staticforward
+#define staticforward static
 #endif
-} DBSequenceObject;
-staticforward PyTypeObject DBSequence_Type;
+#ifndef statichere
+#define statichere static
 #endif
 
-staticforward PyTypeObject DB_Type, DBCursor_Type, DBEnv_Type, DBTxn_Type, DBLock_Type;
+staticforward PyTypeObject DB_Type, DBCursor_Type, DBEnv_Type, DBTxn_Type,
+              DBLock_Type, DBLogCursor_Type;
+staticforward PyTypeObject DBSequence_Type;
+#if (DBVER >= 52)
+staticforward PyTypeObject DBSite_Type;
+#endif
 
-#define DBObject_Check(v)           ((v)->ob_type == &DB_Type)
-#define DBCursorObject_Check(v)     ((v)->ob_type == &DBCursor_Type)
-#define DBEnvObject_Check(v)        ((v)->ob_type == &DBEnv_Type)
-#define DBTxnObject_Check(v)        ((v)->ob_type == &DBTxn_Type)
-#define DBLockObject_Check(v)       ((v)->ob_type == &DBLock_Type)
-#if (DBVER >= 43)
-#define DBSequenceObject_Check(v)   ((v)->ob_type == &DBSequence_Type)
+#ifndef Py_TYPE
+/* for compatibility with Python 2.5 and earlier */
+#define Py_TYPE(ob)              (((PyObject*)(ob))->ob_type)
+#endif
+
+#define DBObject_Check(v)           (Py_TYPE(v) == &DB_Type)
+#define DBCursorObject_Check(v)     (Py_TYPE(v) == &DBCursor_Type)
+#define DBLogCursorObject_Check(v)  (Py_TYPE(v) == &DBLogCursor_Type)
+#define DBEnvObject_Check(v)        (Py_TYPE(v) == &DBEnv_Type)
+#define DBTxnObject_Check(v)        (Py_TYPE(v) == &DBTxn_Type)
+#define DBLockObject_Check(v)       (Py_TYPE(v) == &DBLock_Type)
+#define DBSequenceObject_Check(v)   (Py_TYPE(v) == &DBSequence_Type)
+#if (DBVER >= 52)
+#define DBSiteObject_Check(v)       (Py_TYPE(v) == &DBSite_Type)
+#endif
+
+#if (DBVER < 46)
+  #define _DBC_close(dbc)           dbc->c_close(dbc)
+  #define _DBC_count(dbc,a,b)       dbc->c_count(dbc,a,b)
+  #define _DBC_del(dbc,a)           dbc->c_del(dbc,a)
+  #define _DBC_dup(dbc,a,b)         dbc->c_dup(dbc,a,b)
+  #define _DBC_get(dbc,a,b,c)       dbc->c_get(dbc,a,b,c)
+  #define _DBC_pget(dbc,a,b,c,d)    dbc->c_pget(dbc,a,b,c,d)
+  #define _DBC_put(dbc,a,b,c)       dbc->c_put(dbc,a,b,c)
+#else
+  #define _DBC_close(dbc)           dbc->close(dbc)
+  #define _DBC_count(dbc,a,b)       dbc->count(dbc,a,b)
+  #define _DBC_del(dbc,a)           dbc->del(dbc,a)
+  #define _DBC_dup(dbc,a,b)         dbc->dup(dbc,a,b)
+  #define _DBC_get(dbc,a,b,c)       dbc->get(dbc,a,b,c)
+  #define _DBC_pget(dbc,a,b,c,d)    dbc->pget(dbc,a,b,c,d)
+  #define _DBC_put(dbc,a,b,c)       dbc->put(dbc,a,b,c)
 #endif
 
 
 /* --------------------------------------------------------------------- */
 /* Utility macros and functions */
 
+#define INSERT_IN_DOUBLE_LINKED_LIST(backlink,object)                   \
+    {                                                                   \
+        object->sibling_next=backlink;                                  \
+        object->sibling_prev_p=&(backlink);                             \
+        backlink=object;                                                \
+        if (object->sibling_next) {                                     \
+          object->sibling_next->sibling_prev_p=&(object->sibling_next); \
+        }                                                               \
+    }
+
+#define EXTRACT_FROM_DOUBLE_LINKED_LIST(object)                          \
+    {                                                                    \
+        if (object->sibling_next) {                                      \
+            object->sibling_next->sibling_prev_p=object->sibling_prev_p; \
+        }                                                                \
+        *(object->sibling_prev_p)=object->sibling_next;                  \
+    }
+
+#define EXTRACT_FROM_DOUBLE_LINKED_LIST_MAYBE_NULL(object)               \
+    {                                                                    \
+        if (object->sibling_next) {                                      \
+            object->sibling_next->sibling_prev_p=object->sibling_prev_p; \
+        }                                                                \
+        if (object->sibling_prev_p) {                                    \
+            *(object->sibling_prev_p)=object->sibling_next;              \
+        }                                                                \
+    }
+
+#define INSERT_IN_DOUBLE_LINKED_LIST_TXN(backlink,object)  \
+    {                                                      \
+        object->sibling_next_txn=backlink;                 \
+        object->sibling_prev_p_txn=&(backlink);            \
+        backlink=object;                                   \
+        if (object->sibling_next_txn) {                    \
+            object->sibling_next_txn->sibling_prev_p_txn=  \
+                &(object->sibling_next_txn);               \
+        }                                                  \
+    }
+
+#define EXTRACT_FROM_DOUBLE_LINKED_LIST_TXN(object)             \
+    {                                                           \
+        if (object->sibling_next_txn) {                         \
+            object->sibling_next_txn->sibling_prev_p_txn=       \
+                object->sibling_prev_p_txn;                     \
+        }                                                       \
+        *(object->sibling_prev_p_txn)=object->sibling_next_txn; \
+    }
+
+
 #define RETURN_IF_ERR()          \
     if (makeDBError(err)) {      \
         return NULL;             \
@@ -324,8 +329,10 @@
     if ((nonNull) == NULL) {          \
         PyObject *errTuple = NULL;    \
         errTuple = Py_BuildValue("(is)", 0, #name " object has been closed"); \
-        PyErr_SetObject((pyErrObj), errTuple);  \
-	Py_DECREF(errTuple);          \
+        if (errTuple) { \
+            PyErr_SetObject((pyErrObj), errTuple);  \
+            Py_DECREF(errTuple);          \
+        } \
         return NULL;                  \
     }
 
@@ -338,9 +345,15 @@
 #define CHECK_CURSOR_NOT_CLOSED(curs) \
         _CHECK_OBJECT_NOT_CLOSED(curs->dbc, DBCursorClosedError, DBCursor)
 
-#if (DBVER >= 43)
+#define CHECK_LOGCURSOR_NOT_CLOSED(logcurs) \
+        _CHECK_OBJECT_NOT_CLOSED(logcurs->logc, DBCursorClosedError, DBLogCursor)
+
 #define CHECK_SEQUENCE_NOT_CLOSED(curs) \
         _CHECK_OBJECT_NOT_CLOSED(curs->sequence, DBError, DBSequence)
+
+#if (DBVER >= 52)
+#define CHECK_SITE_NOT_CLOSED(db_site) \
+         _CHECK_OBJECT_NOT_CLOSED(db_site->site, DBError, DBSite)
 #endif
 
 #define CHECK_DBFLAG(mydb, flag)    (((mydb)->flags & (flag)) || \
@@ -358,17 +371,14 @@
 /* Return the access method type of the DBObject */
 static int _DB_get_type(DBObject* self)
 {
-#if (DBVER >= 33)
     DBTYPE type;
     int err;
+
     err = self->db->get_type(self->db, &type);
     if (makeDBError(err)) {
         return -1;
     }
     return type;
-#else
-    return self->db->get_type(self->db);
-#endif
 }
 
 
@@ -382,7 +392,11 @@
     }
     else if (!PyArg_Parse(obj, "s#", &dbt->data, &dbt->size)) {
         PyErr_SetString(PyExc_TypeError,
+#if (PY_VERSION_HEX < 0x03000000)
                         "Data values must be of type string or None.");
+#else
+                        "Data values must be of type bytes or None.");
+#endif
         return 0;
     }
     return 1;
@@ -413,7 +427,7 @@
         /* no need to do anything, the structure has already been zeroed */
     }
 
-    else if (PyString_Check(keyobj)) {
+    else if (PyBytes_Check(keyobj)) {
         /* verify access method type */
         type = _DB_get_type(self);
         if (type == -1)
@@ -421,7 +435,11 @@
         if (type == DB_RECNO || type == DB_QUEUE) {
             PyErr_SetString(
                 PyExc_TypeError,
+#if (PY_VERSION_HEX < 0x03000000)
                 "String keys not allowed for Recno and Queue DB's");
+#else
+                "Bytes keys not allowed for Recno and Queue DB's");
+#endif
             return 0;
         }
 
@@ -432,18 +450,18 @@
          * the code check for DB_THREAD and forceably set DBT_MALLOC
          * when we otherwise would leave flags 0 to indicate that.
          */
-        key->data = malloc(PyString_GET_SIZE(keyobj));
+        key->data = malloc(PyBytes_GET_SIZE(keyobj));
         if (key->data == NULL) {
             PyErr_SetString(PyExc_MemoryError, "Key memory allocation failed");
             return 0;
         }
-        memcpy(key->data, PyString_AS_STRING(keyobj),
-               PyString_GET_SIZE(keyobj));
+        memcpy(key->data, PyBytes_AS_STRING(keyobj),
+               PyBytes_GET_SIZE(keyobj));
         key->flags = DB_DBT_REALLOC;
-        key->size = PyString_GET_SIZE(keyobj);
+        key->size = PyBytes_GET_SIZE(keyobj);
     }
 
-    else if (PyInt_Check(keyobj)) {
+    else if (NUMBER_Check(keyobj)) {
         /* verify access method type */
         type = _DB_get_type(self);
         if (type == -1)
@@ -462,7 +480,7 @@
 
         /* Make a key out of the requested recno, use allocated space so DB
          * will be able to realloc room for the real key if needed. */
-        recno = PyInt_AS_LONG(keyobj);
+        recno = NUMBER_AsLong(keyobj);
         key->data = malloc(sizeof(db_recno_t));
         if (key->data == NULL) {
             PyErr_SetString(PyExc_MemoryError, "Key memory allocation failed");
@@ -474,8 +492,12 @@
     }
     else {
         PyErr_Format(PyExc_TypeError,
+#if (PY_VERSION_HEX < 0x03000000)
                      "String or Integer object expected for key, %s found",
-                     keyobj->ob_type->tp_name);
+#else
+                     "Bytes or Integer object expected for key, %s found",
+#endif
+                     Py_TYPE(keyobj)->tp_name);
         return 0;
     }
 
@@ -511,7 +533,7 @@
 
     srclen = strlen(src);
     if (n <= 0)
-	return srclen;
+        return srclen;
     copylen = (srclen > n-1) ? n-1 : srclen;
     /* populate dest[0] thru dest[copylen-1] */
     memcpy(dest, src, copylen);
@@ -524,17 +546,105 @@
 /* Callback used to save away more information about errors from the DB
  * library. */
 static char _db_errmsg[1024];
-#if (DBVER <= 42)
-static void _db_errorCallback(const char* prefix, char* msg)
-#else
 static void _db_errorCallback(const DB_ENV *db_env,
-	const char* prefix, const char* msg)
-#endif
+        const char* prefix, const char* msg)
 {
     our_strlcpy(_db_errmsg, msg, sizeof(_db_errmsg));
 }
 
 
+/*
+** We need these functions because some results
+** are undefined if pointer is NULL. Some other
+** give None instead of "".
+**
+** This functions are static and will be
+** -I hope- inlined.
+*/
+static const char *DummyString = "This string is a simple placeholder";
+static PyObject *Build_PyString(const char *p,int s)
+{
+  if (!p) {
+    p=DummyString;
+    assert(s==0);
+  }
+  return PyBytes_FromStringAndSize(p,s);
+}
+
+static PyObject *BuildValue_S(const void *p,int s)
+{
+  if (!p) {
+    p=DummyString;
+    assert(s==0);
+  }
+  return PyBytes_FromStringAndSize(p, s);
+}
+
+static PyObject *BuildValue_SS(const void *p1,int s1,const void *p2,int s2)
+{
+PyObject *a, *b, *r;
+
+  if (!p1) {
+    p1=DummyString;
+    assert(s1==0);
+  }
+  if (!p2) {
+    p2=DummyString;
+    assert(s2==0);
+  }
+
+  if (!(a = PyBytes_FromStringAndSize(p1, s1))) {
+      return NULL;
+  }
+  if (!(b = PyBytes_FromStringAndSize(p2, s2))) {
+      Py_DECREF(a);
+      return NULL;
+  }
+
+  r = PyTuple_Pack(2, a, b) ;
+  Py_DECREF(a);
+  Py_DECREF(b);
+  return r;
+}
+
+static PyObject *BuildValue_IS(int i,const void *p,int s)
+{
+  PyObject *a, *r;
+
+  if (!p) {
+    p=DummyString;
+    assert(s==0);
+  }
+
+  if (!(a = PyBytes_FromStringAndSize(p, s))) {
+      return NULL;
+  }
+
+  r = Py_BuildValue("iO", i, a);
+  Py_DECREF(a);
+  return r;
+}
+
+static PyObject *BuildValue_LS(long l,const void *p,int s)
+{
+  PyObject *a, *r;
+
+  if (!p) {
+    p=DummyString;
+    assert(s==0);
+  }
+
+  if (!(a = PyBytes_FromStringAndSize(p, s))) {
+      return NULL;
+  }
+
+  r = Py_BuildValue("lO", l, a);
+  Py_DECREF(a);
+  return r;
+}
+
+
+
 /* make a nice exception object to raise for errors. */
 static int makeDBError(int err)
 {
@@ -545,32 +655,8 @@
     unsigned int bytes_left;
 
     switch (err) {
-        case 0:                     /* successful, no error */      break;
-
-#if (DBVER < 41)
-        case DB_INCOMPLETE:
-#if INCOMPLETE_IS_WARNING
-            bytes_left = our_strlcpy(errTxt, db_strerror(err), sizeof(errTxt));
-            /* Ensure that bytes_left never goes negative */
-            if (_db_errmsg[0] && bytes_left < (sizeof(errTxt) - 4)) {
-                bytes_left = sizeof(errTxt) - bytes_left - 4 - 1;
-		assert(bytes_left >= 0);
-                strcat(errTxt, " -- ");
-                strncat(errTxt, _db_errmsg, bytes_left);
-            }
-            _db_errmsg[0] = 0;
-#ifdef HAVE_WARNINGS
-            exceptionRaised = PyErr_Warn(PyExc_RuntimeWarning, errTxt);
-#else
-            fprintf(stderr, errTxt);
-            fprintf(stderr, "\n");
-#endif
-
-#else  /* do an exception instead */
-        errObj = DBIncompleteError;
-#endif
-        break;
-#endif /* DBVER < 41 */
+        case 0:                     /* successful, no error */
+            return 0;
 
         case DB_KEYEMPTY:           errObj = DBKeyEmptyError;       break;
         case DB_KEYEXIST:           errObj = DBKeyExistError;       break;
@@ -581,18 +667,15 @@
         case DB_RUNRECOVERY:        errObj = DBRunRecoveryError;    break;
         case DB_VERIFY_BAD:         errObj = DBVerifyBadError;      break;
         case DB_NOSERVER:           errObj = DBNoServerError;       break;
+#if (DBVER < 52)
         case DB_NOSERVER_HOME:      errObj = DBNoServerHomeError;   break;
         case DB_NOSERVER_ID:        errObj = DBNoServerIDError;     break;
-#if (DBVER >= 33)
+#endif
         case DB_PAGE_NOTFOUND:      errObj = DBPageNotFoundError;   break;
         case DB_SECONDARY_BAD:      errObj = DBSecondaryBadError;   break;
-#endif
         case DB_BUFFER_SMALL:       errObj = DBNoMemoryError;       break;
 
-#if (DBVER >= 43)
-	/* ENOMEM and DB_BUFFER_SMALL were one and the same until 4.3 */
-	case ENOMEM:  errObj = PyExc_MemoryError;   break;
-#endif
+        case ENOMEM:  errObj = PyExc_MemoryError;   break;
         case EINVAL:  errObj = DBInvalidArgError;   break;
         case EACCES:  errObj = DBAccessError;       break;
         case ENOSPC:  errObj = DBNoSpaceError;      break;
@@ -602,6 +685,21 @@
         case ENOENT:  errObj = DBNoSuchFileError;   break;
         case EPERM :  errObj = DBPermissionsError;  break;
 
+        case DB_REP_HANDLE_DEAD : errObj = DBRepHandleDeadError; break;
+#if (DBVER >= 44)
+        case DB_REP_LOCKOUT : errObj = DBRepLockoutError; break;
+#endif
+
+#if (DBVER >= 46)
+        case DB_REP_LEASE_EXPIRED : errObj = DBRepLeaseExpiredError; break;
+#endif
+
+#if (DBVER >= 47)
+        case DB_FOREIGN_CONFLICT : errObj = DBForeignConflictError; break;
+#endif
+
+        case DB_REP_UNAVAIL : errObj = DBRepUnavailError; break;
+
         default:      errObj = DBError;             break;
     }
 
@@ -616,9 +714,13 @@
         }
         _db_errmsg[0] = 0;
 
-	errTuple = Py_BuildValue("(is)", err, errTxt);
+        errTuple = Py_BuildValue("(is)", err, errTxt);
+        if (errTuple == NULL) {
+            Py_DECREF(errObj);
+            return !0;
+        }
         PyErr_SetObject(errObj, errTuple);
-	Py_DECREF(errTuple);
+        Py_DECREF(errTuple);
     }
 
     return ((errObj != NULL) || exceptionRaised);
@@ -630,7 +732,7 @@
 static void makeTypeError(char* expected, PyObject* found)
 {
     PyErr_Format(PyExc_TypeError, "Expected %s argument, %s found.",
-                 expected, found->ob_type->tp_name);
+                 expected, Py_TYPE(found)->tp_name);
 }
 
 
@@ -663,7 +765,6 @@
     if (makeDBError(err)) {
         return -1;
     }
-    self->haveStat = 0;
     return 0;
 }
 
@@ -680,13 +781,12 @@
     if (makeDBError(err)) {
         return -1;
     }
-    self->haveStat = 0;
     return 0;
 }
 
 /* Get a key/data pair from a cursor */
 static PyObject* _DBCursor_get(DBCursorObject* self, int extra_flags,
-			       PyObject *args, PyObject *kwargs, char *format)
+                               PyObject *args, PyObject *kwargs, char *format)
 {
     int err;
     PyObject* retval = NULL;
@@ -697,7 +797,7 @@
     static char* kwnames[] = { "flags", "dlen", "doff", NULL };
 
     if (!PyArg_ParseTupleAndKeywords(args, kwargs, format, kwnames,
-				     &flags, &dlen, &doff)) 
+                                     &flags, &dlen, &doff))
       return NULL;
 
     CHECK_CURSOR_NOT_CLOSED(self);
@@ -705,20 +805,15 @@
     flags |= extra_flags;
     CLEAR_DBT(key);
     CLEAR_DBT(data);
-    if (CHECK_DBFLAG(self->mydb, DB_THREAD)) {
-        /* Tell BerkeleyDB to malloc the return value (thread safe) */
-        data.flags = DB_DBT_MALLOC;
-        key.flags = DB_DBT_MALLOC;
-    }
     if (!add_partial_dbt(&data, dlen, doff))
         return NULL;
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->dbc->c_get(self->dbc, &key, &data, flags);
+    err = _DBC_get(self->dbc, &key, &data, flags);
     MYDB_END_ALLOW_THREADS;
 
     if ((err == DB_NOTFOUND || err == DB_KEYEMPTY)
-	    && self->mydb->moduleFlags.getReturnsNone) {
+            && self->mydb->moduleFlags.getReturnsNone) {
         Py_INCREF(Py_None);
         retval = Py_None;
     }
@@ -735,21 +830,15 @@
 
         case DB_RECNO:
         case DB_QUEUE:
-            retval = Py_BuildValue("is#", *((db_recno_t*)key.data),
-                                   data.data, data.size);
+            retval = BuildValue_IS(*((db_recno_t*)key.data), data.data, data.size);
             break;
         case DB_HASH:
         case DB_BTREE:
         default:
-            retval = Py_BuildValue("s#s#", key.data, key.size,
-                                   data.data, data.size);
+            retval = BuildValue_SS(key.data, key.size, data.data, data.size);
             break;
         }
     }
-    if (!err) {
-        FREE_DBT(key);
-        FREE_DBT(data);
-    }
     return retval;
 }
 
@@ -757,13 +846,30 @@
 /* add an integer to a dictionary using the given name as a key */
 static void _addIntToDict(PyObject* dict, char *name, int value)
 {
-    PyObject* v = PyInt_FromLong((long) value);
+    PyObject* v = NUMBER_FromLong((long) value);
+    if (!v || PyDict_SetItemString(dict, name, v))
+        PyErr_Clear();
+
+    Py_XDECREF(v);
+}
+
+/* The same, when the value is a time_t */
+static void _addTimeTToDict(PyObject* dict, char *name, time_t value)
+{
+    PyObject* v;
+        /* if the value fits in regular int, use that. */
+#ifdef PY_LONG_LONG
+        if (sizeof(time_t) > sizeof(long))
+                v = PyLong_FromLongLong((PY_LONG_LONG) value);
+        else
+#endif
+                v = NUMBER_FromLong((long) value);
     if (!v || PyDict_SetItemString(dict, name, v))
         PyErr_Clear();
 
     Py_XDECREF(v);
 }
-#if (DBVER >= 43)
+
 /* add an db_seq_t to a dictionary using the given name as a key */
 static void _addDb_seq_tToDict(PyObject* dict, char *name, db_seq_t value)
 {
@@ -773,9 +879,15 @@
 
     Py_XDECREF(v);
 }
-#endif
 
+static void _addDB_lsnToDict(PyObject* dict, char *name, DB_LSN value)
+{
+    PyObject *v = Py_BuildValue("(ll)",value.file,value.offset);
+    if (!v || PyDict_SetItemString(dict, name, v))
+        PyErr_Clear();
 
+    Py_XDECREF(v);
+}
 
 /* --------------------------------------------------------------------- */
 /* Allocators and deallocators */
@@ -791,25 +903,33 @@
     if (self == NULL)
         return NULL;
 
-    self->haveStat = 0;
     self->flags = 0;
     self->setflags = 0;
     self->myenvobj = NULL;
-#if (DBVER >= 33)
+    self->db = NULL;
+    self->children_cursors = NULL;
+    self->children_sequences = NULL;
     self->associateCallback = NULL;
     self->btCompareCallback = NULL;
+    self->dupCompareCallback = NULL;
     self->primaryDBType = 0;
-#endif
-#ifdef HAVE_WEAKREF
+    Py_INCREF(Py_None);
+    self->private_obj = Py_None;
     self->in_weakreflist = NULL;
-#endif
 
     /* keep a reference to our python DBEnv object */
     if (arg) {
         Py_INCREF(arg);
         self->myenvobj = arg;
         db_env = arg->db_env;
+        INSERT_IN_DOUBLE_LINKED_LIST(self->myenvobj->children_dbs,self);
+    } else {
+      self->sibling_prev_p=NULL;
+      self->sibling_next=NULL;
     }
+    self->txn=NULL;
+    self->sibling_prev_p_txn=NULL;
+    self->sibling_next_txn=NULL;
 
     if (self->myenvobj)
         self->moduleFlags = self->myenvobj->moduleFlags;
@@ -821,9 +941,7 @@
     err = db_create(&self->db, db_env, flags);
     if (self->db != NULL) {
         self->db->set_errcall(self->db, _db_errorCallback);
-#if (DBVER >= 33)
         self->db->app_private = (void*)self;
-#endif
     }
     MYDB_END_ALLOW_THREADS;
     /* TODO add a weakref(self) to the self->myenvobj->open_child_weakrefs
@@ -841,36 +959,32 @@
 }
 
 
+/* Forward declaration */
+static PyObject *DB_close_internal(DBObject* self, int flags, int do_not_close);
+
 static void
 DB_dealloc(DBObject* self)
 {
+  PyObject *dummy;
+
     if (self->db != NULL) {
-        /* avoid closing a DB when its DBEnv has been closed out from under
-         * it */
-        if (!self->myenvobj ||
-            (self->myenvobj && self->myenvobj->db_env))
-        {
-            MYDB_BEGIN_ALLOW_THREADS;
-            self->db->close(self->db, 0);
-            MYDB_END_ALLOW_THREADS;
-#ifdef HAVE_WARNINGS
-        } else {
-            PyErr_Warn(PyExc_RuntimeWarning,
-                "DB could not be closed in destructor: DBEnv already closed");
-#endif
-        }
-        self->db = NULL;
+        dummy=DB_close_internal(self, 0, 0);
+        /*
+        ** Raising exceptions while doing
+        ** garbage collection is a fatal error.
+        */
+        if (dummy)
+            Py_DECREF(dummy);
+        else
+            PyErr_Clear();
     }
-#ifdef HAVE_WEAKREF
     if (self->in_weakreflist != NULL) {
         PyObject_ClearWeakRefs((PyObject *) self);
     }
-#endif
     if (self->myenvobj) {
         Py_DECREF(self->myenvobj);
         self->myenvobj = NULL;
     }
-#if (DBVER >= 33)
     if (self->associateCallback != NULL) {
         Py_DECREF(self->associateCallback);
         self->associateCallback = NULL;
@@ -879,13 +993,16 @@
         Py_DECREF(self->btCompareCallback);
         self->btCompareCallback = NULL;
     }
-#endif
+    if (self->dupCompareCallback != NULL) {
+        Py_DECREF(self->dupCompareCallback);
+        self->dupCompareCallback = NULL;
+    }
+    Py_DECREF(self->private_obj);
     PyObject_Del(self);
 }
 
-
 static DBCursorObject*
-newDBCursorObject(DBC* dbc, DBObject* db)
+newDBCursorObject(DBC* dbc, DBTxnObject *txn, DBObject* db)
 {
     DBCursorObject* self = PyObject_New(DBCursorObject, &DBCursor_Type);
     if (self == NULL)
@@ -893,44 +1010,92 @@
 
     self->dbc = dbc;
     self->mydb = db;
-#ifdef HAVE_WEAKREF
+
+    INSERT_IN_DOUBLE_LINKED_LIST(self->mydb->children_cursors,self);
+    if (txn && ((PyObject *)txn!=Py_None)) {
+            INSERT_IN_DOUBLE_LINKED_LIST_TXN(txn->children_cursors,self);
+            self->txn=txn;
+    } else {
+            self->txn=NULL;
+    }
+
     self->in_weakreflist = NULL;
-#endif
     Py_INCREF(self->mydb);
     return self;
 }
 
 
+/* Forward declaration */
+static PyObject *DBC_close_internal(DBCursorObject* self);
+
 static void
 DBCursor_dealloc(DBCursorObject* self)
 {
-    int err;
+    PyObject *dummy;
 
-#ifdef HAVE_WEAKREF
+    if (self->dbc != NULL) {
+        dummy=DBC_close_internal(self);
+        /*
+        ** Raising exceptions while doing
+        ** garbage collection is a fatal error.
+        */
+        if (dummy)
+            Py_DECREF(dummy);
+        else
+            PyErr_Clear();
+    }
     if (self->in_weakreflist != NULL) {
         PyObject_ClearWeakRefs((PyObject *) self);
     }
-#endif
+    Py_DECREF(self->mydb);
+    PyObject_Del(self);
+}
 
-    if (self->dbc != NULL) {
-	/* If the underlying database has been closed, we don't
-	   need to do anything. If the environment has been closed
-	   we need to leak, as BerkeleyDB will crash trying to access
-	   the environment. There was an exception when the 
-	   user closed the environment even though there still was
-	   a database open. */
-	if (self->mydb->db && self->mydb->myenvobj &&
-	    !self->mydb->myenvobj->closed)
-        /* test for: open db + no environment or non-closed environment */
-	if (self->mydb->db && (!self->mydb->myenvobj || (self->mydb->myenvobj &&
-	    !self->mydb->myenvobj->closed))) {
-            MYDB_BEGIN_ALLOW_THREADS;
-            err = self->dbc->c_close(self->dbc);
-            MYDB_END_ALLOW_THREADS;
-        }
-        self->dbc = NULL;
+
+static DBLogCursorObject*
+newDBLogCursorObject(DB_LOGC* dblogc, DBEnvObject* env)
+{
+    DBLogCursorObject* self;
+
+    self = PyObject_New(DBLogCursorObject, &DBLogCursor_Type);
+
+    if (self == NULL)
+        return NULL;
+
+    self->logc = dblogc;
+    self->env = env;
+
+    INSERT_IN_DOUBLE_LINKED_LIST(self->env->children_logcursors, self);
+
+    self->in_weakreflist = NULL;
+    Py_INCREF(self->env);
+    return self;
+}
+
+
+/* Forward declaration */
+static PyObject *DBLogCursor_close_internal(DBLogCursorObject* self);
+
+static void
+DBLogCursor_dealloc(DBLogCursorObject* self)
+{
+    PyObject *dummy;
+
+    if (self->logc != NULL) {
+        dummy = DBLogCursor_close_internal(self);
+        /*
+        ** Raising exceptions while doing
+        ** garbage collection is a fatal error.
+        */
+        if (dummy)
+            Py_DECREF(dummy);
+        else
+            PyErr_Clear();
+    }
+    if (self->in_weakreflist != NULL) {
+        PyObject_ClearWeakRefs((PyObject *) self);
     }
-    Py_XDECREF( self->mydb );
+    Py_DECREF(self->env);
     PyObject_Del(self);
 }
 
@@ -943,13 +1108,23 @@
     if (self == NULL)
         return NULL;
 
+    self->db_env = NULL;
     self->closed = 1;
     self->flags = flags;
     self->moduleFlags.getReturnsNone = DEFAULT_GET_RETURNS_NONE;
     self->moduleFlags.cursorSetReturnsNone = DEFAULT_CURSOR_SET_RETURNS_NONE;
-#ifdef HAVE_WEAKREF
+    self->children_dbs = NULL;
+    self->children_txns = NULL;
+    self->children_logcursors = NULL ;
+#if (DBVER >= 52)
+    self->children_sites = NULL;
+#endif
+    Py_INCREF(Py_None);
+    self->private_obj = Py_None;
+    Py_INCREF(Py_None);
+    self->rep_transport = Py_None;
     self->in_weakreflist = NULL;
-#endif
+    self->event_notifyCallback = NULL;
 
     MYDB_BEGIN_ALLOW_THREADS;
     err = db_env_create(&self->db_env, flags);
@@ -960,84 +1135,142 @@
     }
     else {
         self->db_env->set_errcall(self->db_env, _db_errorCallback);
+        self->db_env->app_private = self;
     }
     return self;
 }
 
+/* Forward declaration */
+static PyObject *DBEnv_close_internal(DBEnvObject* self, int flags);
 
 static void
 DBEnv_dealloc(DBEnvObject* self)
 {
-#ifdef HAVE_WEAKREF
-    if (self->in_weakreflist != NULL) {
-        PyObject_ClearWeakRefs((PyObject *) self);
+  PyObject *dummy;
+
+    if (self->db_env) {
+        dummy=DBEnv_close_internal(self, 0);
+        /*
+        ** Raising exceptions while doing
+        ** garbage collection is a fatal error.
+        */
+        if (dummy)
+            Py_DECREF(dummy);
+        else
+            PyErr_Clear();
     }
-#endif
 
-    if (self->db_env && !self->closed) {
-        MYDB_BEGIN_ALLOW_THREADS;
-        self->db_env->close(self->db_env, 0);
-        MYDB_END_ALLOW_THREADS;
+    Py_XDECREF(self->event_notifyCallback);
+    self->event_notifyCallback = NULL;
+
+    if (self->in_weakreflist != NULL) {
+        PyObject_ClearWeakRefs((PyObject *) self);
     }
+    Py_DECREF(self->private_obj);
+    Py_DECREF(self->rep_transport);
     PyObject_Del(self);
 }
 
 
 static DBTxnObject*
-newDBTxnObject(DBEnvObject* myenv, DB_TXN *parent, int flags)
+newDBTxnObject(DBEnvObject* myenv, DBTxnObject *parent, DB_TXN *txn, int flags)
 {
     int err;
+    DB_TXN *parent_txn = NULL;
+
     DBTxnObject* self = PyObject_New(DBTxnObject, &DBTxn_Type);
     if (self == NULL)
         return NULL;
-    Py_INCREF(myenv);
-    self->env = (PyObject*)myenv;
-#ifdef HAVE_WEAKREF
+
     self->in_weakreflist = NULL;
-#endif
+    self->children_txns = NULL;
+    self->children_dbs = NULL;
+    self->children_cursors = NULL;
+    self->children_sequences = NULL;
+    self->flag_prepare = 0;
+    self->parent_txn = NULL;
+    self->env = NULL;
+    /* We initialize just in case "txn_begin" fails */
+    self->txn = NULL;
 
-    MYDB_BEGIN_ALLOW_THREADS;
-#if (DBVER >= 40)
-    err = myenv->db_env->txn_begin(myenv->db_env, parent, &(self->txn), flags);
-#else
-    err = txn_begin(myenv->db_env, parent, &(self->txn), flags);
-#endif
-    MYDB_END_ALLOW_THREADS;
-    if (makeDBError(err)) {
-        Py_DECREF(self);
-        self = NULL;
+    if (parent && ((PyObject *)parent!=Py_None)) {
+        parent_txn = parent->txn;
+    }
+
+    if (txn) {
+        self->txn = txn;
+    } else {
+        MYDB_BEGIN_ALLOW_THREADS;
+        err = myenv->db_env->txn_begin(myenv->db_env, parent_txn, &(self->txn), flags);
+        MYDB_END_ALLOW_THREADS;
+
+        if (makeDBError(err)) {
+            /* Free object half initialized */
+            Py_DECREF(self);
+            return NULL;
+        }
+    }
+
+    /* Can't use 'parent' because could be 'parent==Py_None' */
+    if (parent_txn) {
+        self->parent_txn = parent;
+        Py_INCREF(parent);
+        self->env = NULL;
+        INSERT_IN_DOUBLE_LINKED_LIST(parent->children_txns, self);
+    } else {
+        self->parent_txn = NULL;
+        Py_INCREF(myenv);
+        self->env = myenv;
+        INSERT_IN_DOUBLE_LINKED_LIST(myenv->children_txns, self);
     }
+
     return self;
 }
 
+/* Forward declaration */
+static PyObject *
+DBTxn_abort_discard_internal(DBTxnObject* self, int discard);
 
 static void
 DBTxn_dealloc(DBTxnObject* self)
 {
-#ifdef HAVE_WEAKREF
-    if (self->in_weakreflist != NULL) {
-        PyObject_ClearWeakRefs((PyObject *) self);
-    }
-#endif
+  PyObject *dummy;
 
-#ifdef HAVE_WARNINGS
     if (self->txn) {
-        /* it hasn't been finalized, abort it! */
-        MYDB_BEGIN_ALLOW_THREADS;
-#if (DBVER >= 40)
-        self->txn->abort(self->txn);
-#else
-        txn_abort(self->txn);
-#endif
-        MYDB_END_ALLOW_THREADS;
-        PyErr_Warn(PyExc_RuntimeWarning,
-            "DBTxn aborted in destructor.  No prior commit() or abort().");
-    }
-#endif
-
-    Py_DECREF(self->env);
-    PyObject_Del(self);
-}
+        int flag_prepare = self->flag_prepare;
+
+        dummy=DBTxn_abort_discard_internal(self, 0);
+        /*
+        ** Raising exceptions while doing
+        ** garbage collection is a fatal error.
+        */
+        if (dummy)
+            Py_DECREF(dummy);
+        else
+            PyErr_Clear();
+
+        if (!flag_prepare) {
+            PyErr_Warn(PyExc_RuntimeWarning,
+              "DBTxn aborted in destructor.  No prior commit() or abort().");
+        }
+    }
+
+    if (self->in_weakreflist != NULL) {
+        PyObject_ClearWeakRefs((PyObject *) self);
+    }
+
+    if (self->env) {
+        Py_DECREF(self->env);
+    } else {
+        /*
+        ** We can have "self->env==NULL" and "self->parent_txn==NULL"
+        ** if something happens when creating the transaction object
+        ** and we abort the object while half done.
+        */
+        Py_XDECREF(self->parent_txn);
+    }
+    PyObject_Del(self);
+}
 
 
 static DBLockObject*
@@ -1048,21 +1281,18 @@
     DBLockObject* self = PyObject_New(DBLockObject, &DBLock_Type);
     if (self == NULL)
         return NULL;
-#ifdef HAVE_WEAKREF
     self->in_weakreflist = NULL;
-#endif
+    self->lock_initialized = 0;  /* Just in case the call fails */
 
     MYDB_BEGIN_ALLOW_THREADS;
-#if (DBVER >= 40)
     err = myenv->db_env->lock_get(myenv->db_env, locker, flags, obj, lock_mode,
                                   &self->lock);
-#else
-    err = lock_get(myenv->db_env, locker, flags, obj, lock_mode, &self->lock);
-#endif
     MYDB_END_ALLOW_THREADS;
     if (makeDBError(err)) {
         Py_DECREF(self);
         self = NULL;
+    } else {
+        self->lock_initialized = 1;
     }
 
     return self;
@@ -1072,18 +1302,16 @@
 static void
 DBLock_dealloc(DBLockObject* self)
 {
-#ifdef HAVE_WEAKREF
     if (self->in_weakreflist != NULL) {
         PyObject_ClearWeakRefs((PyObject *) self);
     }
-#endif
     /* TODO: is this lock held? should we release it? */
+    /* CAUTION: The lock can be not initialized if the creation has failed */
 
     PyObject_Del(self);
 }
 
 
-#if (DBVER >= 43)
 static DBSequenceObject*
 newDBSequenceObject(DBObject* mydb,  int flags)
 {
@@ -1093,10 +1321,12 @@
         return NULL;
     Py_INCREF(mydb);
     self->mydb = mydb;
-#ifdef HAVE_WEAKREF
-    self->in_weakreflist = NULL;
-#endif
 
+    INSERT_IN_DOUBLE_LINKED_LIST(self->mydb->children_sequences,self);
+    self->txn = NULL;
+
+    self->in_weakreflist = NULL;
+    self->sequence = NULL;  /* Just in case the call fails */
 
     MYDB_BEGIN_ALLOW_THREADS;
     err = db_sequence_create(&self->sequence, self->mydb->db, flags);
@@ -1109,34 +1339,98 @@
     return self;
 }
 
+/* Forward declaration */
+static PyObject
+*DBSequence_close_internal(DBSequenceObject* self, int flags, int do_not_close);
 
 static void
 DBSequence_dealloc(DBSequenceObject* self)
 {
-#ifdef HAVE_WEAKREF
+    PyObject *dummy;
+
+    if (self->sequence != NULL) {
+        dummy=DBSequence_close_internal(self,0,0);
+        /*
+        ** Raising exceptions while doing
+        ** garbage collection is a fatal error.
+        */
+        if (dummy)
+            Py_DECREF(dummy);
+        else
+            PyErr_Clear();
+    }
+
     if (self->in_weakreflist != NULL) {
         PyObject_ClearWeakRefs((PyObject *) self);
     }
-#endif
 
     Py_DECREF(self->mydb);
     PyObject_Del(self);
 }
+
+#if (DBVER >= 52)
+static DBSiteObject*
+newDBSiteObject(DB_SITE* sitep, DBEnvObject* env)
+{
+    DBSiteObject* self;
+
+    self = PyObject_New(DBSiteObject, &DBSite_Type);
+
+    if (self == NULL)
+        return NULL;
+
+    self->site = sitep;
+    self->env = env;
+
+    INSERT_IN_DOUBLE_LINKED_LIST(self->env->children_sites, self);
+
+    self->in_weakreflist = NULL;
+    Py_INCREF(self->env);
+    return self;
+}
+
+/* Forward declaration */
+static PyObject *DBSite_close_internal(DBSiteObject* self);
+
+static void
+DBSite_dealloc(DBSiteObject* self)
+{
+    PyObject *dummy;
+
+    if (self->site != NULL) {
+        dummy = DBSite_close_internal(self);
+        /*
+        ** Raising exceptions while doing
+        ** garbage collection is a fatal error.
+        */
+        if (dummy)
+            Py_DECREF(dummy);
+        else
+            PyErr_Clear();
+    }
+    if (self->in_weakreflist != NULL) {
+        PyObject_ClearWeakRefs((PyObject *) self);
+    }
+    Py_DECREF(self->env);
+    PyObject_Del(self);
+}
 #endif
 
 /* --------------------------------------------------------------------- */
 /* DB methods */
 
 static PyObject*
-DB_append(DBObject* self, PyObject* args)
+DB_append(DBObject* self, PyObject* args, PyObject* kwargs)
 {
     PyObject* txnobj = NULL;
     PyObject* dataobj;
     db_recno_t recno;
     DBT key, data;
     DB_TXN *txn = NULL;
+    static char* kwnames[] = { "data", "txn", NULL };
 
-    if (!PyArg_UnpackTuple(args, "append", 1, 2, &dataobj, &txnobj))
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|O:append", kwnames,
+                                     &dataobj, &txnobj))
         return NULL;
 
     CHECK_DB_NOT_CLOSED(self);
@@ -1155,12 +1449,10 @@
     if (-1 == _DB_put(self, txn, &key, &data, DB_APPEND))
         return NULL;
 
-    return PyInt_FromLong(recno);
+    return NUMBER_FromLong(recno);
 }
 
 
-#if (DBVER >= 33)
-
 static int
 _db_associateCallback(DB* db, const DBT* priKey, const DBT* priData,
                       DBT* secKey)
@@ -1177,11 +1469,9 @@
         MYDB_BEGIN_BLOCK_THREADS;
 
         if (type == DB_RECNO || type == DB_QUEUE)
-            args = Py_BuildValue("(ls#)", *((db_recno_t*)priKey->data),
-                                 priData->data, priData->size);
+            args = BuildValue_LS(*((db_recno_t*)priKey->data), priData->data, priData->size);
         else
-            args = Py_BuildValue("(s#s#)", priKey->data, priKey->size,
-                                 priData->data, priData->size);
+            args = BuildValue_SS(priKey->data, priKey->size, priData->data, priData->size);
         if (args != NULL) {
                 result = PyEval_CallObject(callback, args);
         }
@@ -1191,38 +1481,92 @@
         else if (result == Py_None) {
             retval = DB_DONOTINDEX;
         }
-        else if (PyInt_Check(result)) {
-            retval = PyInt_AsLong(result);
+        else if (NUMBER_Check(result)) {
+            retval = NUMBER_AsLong(result);
         }
-        else if (PyString_Check(result)) {
+        else if (PyBytes_Check(result)) {
             char* data;
             Py_ssize_t size;
 
             CLEAR_DBT(*secKey);
-#if PYTHON_API_VERSION <= 1007
-            /* 1.5 compatibility */
-            size = PyString_Size(result);
-            data = PyString_AsString(result);
-#else
-            PyString_AsStringAndSize(result, &data, &size);
-#endif
+            PyBytes_AsStringAndSize(result, &data, &size);
             secKey->flags = DB_DBT_APPMALLOC;   /* DB will free */
             secKey->data = malloc(size);        /* TODO, check this */
-	    if (secKey->data) {
-		memcpy(secKey->data, data, size);
-		secKey->size = size;
-		retval = 0;
-	    }
-	    else {
-		PyErr_SetString(PyExc_MemoryError,
+            if (secKey->data) {
+                memcpy(secKey->data, data, size);
+                secKey->size = size;
+                retval = 0;
+            }
+            else {
+                PyErr_SetString(PyExc_MemoryError,
                                 "malloc failed in _db_associateCallback");
-		PyErr_Print();
-	    }
+                PyErr_Print();
+            }
+        }
+#if (DBVER >= 46)
+        else if (PyList_Check(result))
+        {
+            char* data;
+            Py_ssize_t size;
+            int i, listlen;
+            DBT* dbts;
+
+            listlen = PyList_Size(result);
+
+            dbts = (DBT *)malloc(sizeof(DBT) * listlen);
+
+            for (i=0; i<listlen; i++)
+            {
+                if (!PyBytes_Check(PyList_GetItem(result, i)))
+                {
+                    PyErr_SetString(
+                       PyExc_TypeError,
+#if (PY_VERSION_HEX < 0x03000000)
+"The list returned by DB->associate callback should be a list of strings.");
+#else
+"The list returned by DB->associate callback should be a list of bytes.");
+#endif
+                    PyErr_Print();
+                }
+
+                PyBytes_AsStringAndSize(
+                    PyList_GetItem(result, i),
+                    &data, &size);
+
+                CLEAR_DBT(dbts[i]);
+                dbts[i].data = malloc(size);          /* TODO, check this */
+
+                if (dbts[i].data)
+                {
+                    memcpy(dbts[i].data, data, size);
+                    dbts[i].size = size;
+                    dbts[i].ulen = dbts[i].size;
+                    dbts[i].flags = DB_DBT_APPMALLOC;  /* DB will free */
+                }
+                else
+                {
+                    PyErr_SetString(PyExc_MemoryError,
+                        "malloc failed in _db_associateCallback (list)");
+                    PyErr_Print();
+                }
+            }
+
+            CLEAR_DBT(*secKey);
+
+            secKey->data = dbts;
+            secKey->size = listlen;
+            secKey->flags = DB_DBT_APPMALLOC | DB_DBT_MULTIPLE;
+            retval = 0;
         }
+#endif
         else {
             PyErr_SetString(
                PyExc_TypeError,
-               "DB associate callback should return DB_DONOTINDEX or string.");
+#if (PY_VERSION_HEX < 0x03000000)
+"DB associate callback should return DB_DONOTINDEX/string/list of strings.");
+#else
+"DB associate callback should return DB_DONOTINDEX/bytes/list of bytes.");
+#endif
             PyErr_Print();
         }
 
@@ -1241,29 +1585,18 @@
     int err, flags=0;
     DBObject* secondaryDB;
     PyObject* callback;
-#if (DBVER >= 41)
     PyObject *txnobj = NULL;
     DB_TXN *txn = NULL;
     static char* kwnames[] = {"secondaryDB", "callback", "flags", "txn",
                                     NULL};
-#else
-    static char* kwnames[] = {"secondaryDB", "callback", "flags", NULL};
-#endif
 
-#if (DBVER >= 41)
     if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|iO:associate", kwnames,
                                      &secondaryDB, &callback, &flags,
                                      &txnobj)) {
-#else
-    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|i:associate", kwnames,
-                                     &secondaryDB, &callback, &flags)) {
-#endif
         return NULL;
     }
 
-#if (DBVER >= 41)
     if (!checkTxnObj(txnobj, &txn)) return NULL;
-#endif
 
     CHECK_DB_NOT_CLOSED(self);
     if (!DBObject_Check(secondaryDB)) {
@@ -1299,18 +1632,11 @@
     PyEval_InitThreads();
 #endif
     MYDB_BEGIN_ALLOW_THREADS;
-#if (DBVER >= 41)
-    err = self->db->associate(self->db,
-	                      txn,
-                              secondaryDB->db,
-                              _db_associateCallback,
-                              flags);
-#else
     err = self->db->associate(self->db,
+                              txn,
                               secondaryDB->db,
                               _db_associateCallback,
                               flags);
-#endif
     MYDB_END_ALLOW_THREADS;
 
     if (err) {
@@ -1324,27 +1650,60 @@
 }
 
 
-#endif
+static PyObject*
+DB_close_internal(DBObject* self, int flags, int do_not_close)
+{
+    PyObject *dummy;
+    int err = 0;
+
+    if (self->db != NULL) {
+        /* Can be NULL if db is not in an environment */
+        EXTRACT_FROM_DOUBLE_LINKED_LIST_MAYBE_NULL(self);
+
+        if (self->txn) {
+            EXTRACT_FROM_DOUBLE_LINKED_LIST_TXN(self);
+            self->txn=NULL;
+        }
+
+        while(self->children_cursors) {
+          dummy=DBC_close_internal(self->children_cursors);
+          Py_XDECREF(dummy);
+        }
+
+        while(self->children_sequences) {
+            dummy=DBSequence_close_internal(self->children_sequences,0,0);
+            Py_XDECREF(dummy);
+        }
 
+        /*
+        ** "do_not_close" is used to dispose all related objects in the
+        ** tree, without actually releasing the "root" object.
+        ** This is done, for example, because function calls like
+        ** "DB.verify()" implicitly close the underlying handle. So
+        ** the handle doesn't need to be closed, but related objects
+        ** must be cleaned up.
+        */
+        if (!do_not_close) {
+            MYDB_BEGIN_ALLOW_THREADS;
+            err = self->db->close(self->db, flags);
+            MYDB_END_ALLOW_THREADS;
+            self->db = NULL;
+        }
+        RETURN_IF_ERR();
+    }
+    RETURN_NONE();
+}
 
 static PyObject*
 DB_close(DBObject* self, PyObject* args)
 {
-    int err, flags=0;
+    int flags=0;
     if (!PyArg_ParseTuple(args,"|i:close", &flags))
         return NULL;
-    if (self->db != NULL) {
-        if (self->myenvobj)
-            CHECK_ENV_NOT_CLOSED(self->myenvobj);
-        err = self->db->close(self->db, flags);
-        self->db = NULL;
-        RETURN_IF_ERR();
-    }
-    RETURN_NONE();
+    return DB_close_internal(self, flags, 0);
 }
 
 
-#if (DBVER >= 32)
 static PyObject*
 _DB_consume(DBObject* self, PyObject* args, PyObject* kwargs, int consume_flag)
 {
@@ -1374,7 +1733,7 @@
     CLEAR_DBT(key);
     CLEAR_DBT(data);
     if (CHECK_DBFLAG(self, DB_THREAD)) {
-        /* Tell BerkeleyDB to malloc the return value (thread safe) */
+        /* Tell Berkeley DB to malloc the return value (thread safe) */
         data.flags = DB_DBT_MALLOC;
         key.flags = DB_DBT_MALLOC;
     }
@@ -1384,14 +1743,13 @@
     MYDB_END_ALLOW_THREADS;
 
     if ((err == DB_NOTFOUND || err == DB_KEYEMPTY)
-	    && self->moduleFlags.getReturnsNone) {
+            && self->moduleFlags.getReturnsNone) {
         err = 0;
         Py_INCREF(Py_None);
         retval = Py_None;
     }
     else if (!err) {
-        retval = Py_BuildValue("s#s#", key.data, key.size, data.data,
-                               data.size);
+        retval = BuildValue_SS(key.data, key.size, data.data, data.size);
         FREE_DBT(key);
         FREE_DBT(data);
     }
@@ -1412,8 +1770,6 @@
 {
     return _DB_consume(self, args, kwargs, DB_CONSUME_WAIT);
 }
-#endif
-
 
 
 static PyObject*
@@ -1436,7 +1792,7 @@
     err = self->db->cursor(self->db, txn, &dbc, flags);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
-    return (PyObject*) newDBCursorObject(dbc, self);
+    return (PyObject*) newDBCursorObject(dbc, (DBTxnObject *)txnobj, self);
 }
 
 
@@ -1471,23 +1827,128 @@
 }
 
 
+#if (DBVER >= 47)
+/*
+** This function is available since Berkeley DB 4.4,
+** but 4.6 version is so buggy that we only support
+** it from BDB 4.7 and newer.
+*/
 static PyObject*
-DB_fd(DBObject* self, PyObject* args)
+DB_compact(DBObject* self, PyObject* args, PyObject* kwargs)
 {
-    int err, the_fd;
+    PyObject* txnobj = NULL;
+    PyObject *startobj = NULL, *stopobj = NULL;
+    int flags = 0;
+    DB_TXN *txn = NULL;
+    DBT *start_p = NULL, *stop_p = NULL;
+    DBT start, stop;
+    int err;
+    DB_COMPACT c_data = { 0 };
+    static char* kwnames[] = { "txn", "start", "stop", "flags",
+                               "compact_fillpercent", "compact_pages",
+                               "compact_timeout", NULL };
+
 
-    if (!PyArg_ParseTuple(args,":fd"))
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OOOiiiI:compact", kwnames,
+                                     &txnobj, &startobj, &stopobj, &flags,
+                                     &c_data.compact_fillpercent,
+                                     &c_data.compact_pages,
+                                     &c_data.compact_timeout))
+        return NULL;
+
+    CHECK_DB_NOT_CLOSED(self);
+    if (!checkTxnObj(txnobj, &txn)) {
         return NULL;
+    }
+
+    if (startobj && make_key_dbt(self, startobj, &start, NULL)) {
+        start_p = &start;
+    }
+    if (stopobj && make_key_dbt(self, stopobj, &stop, NULL)) {
+        stop_p = &stop;
+    }
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->compact(self->db, txn, start_p, stop_p, &c_data,
+                            flags, NULL);
+    MYDB_END_ALLOW_THREADS;
+
+    if (startobj)
+        FREE_DBT(start);
+    if (stopobj)
+        FREE_DBT(stop);
+
+    RETURN_IF_ERR();
+
+    return PyLong_FromUnsignedLong(c_data.compact_pages_truncated);
+}
+#endif
+
+
+static PyObject*
+DB_fd(DBObject* self)
+{
+    int err, the_fd;
+
     CHECK_DB_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
     err = self->db->fd(self->db, &the_fd);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
-    return PyInt_FromLong(the_fd);
+    return NUMBER_FromLong(the_fd);
 }
 
 
+#if (DBVER >= 46)
+static PyObject*
+DB_exists(DBObject* self, PyObject* args, PyObject* kwargs)
+{
+    int err, flags=0;
+    PyObject* txnobj = NULL;
+    PyObject* keyobj;
+    DBT key;
+    DB_TXN *txn;
+
+    static char* kwnames[] = {"key", "txn", "flags", NULL};
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|Oi:exists", kwnames,
+                &keyobj, &txnobj, &flags))
+        return NULL;
+
+    CHECK_DB_NOT_CLOSED(self);
+    if (!make_key_dbt(self, keyobj, &key, NULL))
+        return NULL;
+    if (!checkTxnObj(txnobj, &txn)) {
+        FREE_DBT(key);
+        return NULL;
+    }
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->exists(self->db, txn, &key, flags);
+    MYDB_END_ALLOW_THREADS;
+
+    FREE_DBT(key);
+
+    if (!err) {
+        Py_INCREF(Py_True);
+        return Py_True;
+    }
+    if ((err == DB_NOTFOUND || err == DB_KEYEMPTY)) {
+        Py_INCREF(Py_False);
+        return Py_False;
+    }
+
+    /*
+    ** If we reach there, there was an error. The
+    ** "return" should be unreachable.
+    */
+    RETURN_IF_ERR();
+    assert(0);  /* This coude SHOULD be unreachable */
+    return NULL;
+}
+#endif
+
 static PyObject*
 DB_get(DBObject* self, PyObject* args, PyObject* kwargs)
 {
@@ -1518,7 +1979,7 @@
 
     CLEAR_DBT(data);
     if (CHECK_DBFLAG(self, DB_THREAD)) {
-        /* Tell BerkeleyDB to malloc the return value (thread safe) */
+        /* Tell Berkeley DB to malloc the return value (thread safe) */
         data.flags = DB_DBT_MALLOC;
     }
     if (!add_partial_dbt(&data, dlen, doff)) {
@@ -1536,17 +1997,16 @@
         retval = dfltobj;
     }
     else if ((err == DB_NOTFOUND || err == DB_KEYEMPTY)
-	     && self->moduleFlags.getReturnsNone) {
+             && self->moduleFlags.getReturnsNone) {
         err = 0;
         Py_INCREF(Py_None);
         retval = Py_None;
     }
     else if (!err) {
         if (flags & DB_SET_RECNO) /* return both key and data */
-            retval = Py_BuildValue("s#s#", key.data, key.size, data.data,
-                                   data.size);
+            retval = BuildValue_SS(key.data, key.size, data.data, data.size);
         else /* return just the data */
-            retval = PyString_FromStringAndSize((char*)data.data, data.size);
+            retval = Build_PyString(data.data, data.size);
         FREE_DBT(data);
     }
     FREE_DBT(key);
@@ -1555,7 +2015,6 @@
     return retval;
 }
 
-#if (DBVER >= 33)
 static PyObject*
 DB_pget(DBObject* self, PyObject* args, PyObject* kwargs)
 {
@@ -1586,7 +2045,7 @@
 
     CLEAR_DBT(data);
     if (CHECK_DBFLAG(self, DB_THREAD)) {
-        /* Tell BerkeleyDB to malloc the return value (thread safe) */
+        /* Tell Berkeley DB to malloc the return value (thread safe) */
         data.flags = DB_DBT_MALLOC;
     }
     if (!add_partial_dbt(&data, dlen, doff)) {
@@ -1596,7 +2055,7 @@
 
     CLEAR_DBT(pkey);
     pkey.flags = DB_DBT_MALLOC;
-    
+
     MYDB_BEGIN_ALLOW_THREADS;
     err = self->db->pget(self->db, txn, &key, &pkey, &data, flags);
     MYDB_END_ALLOW_THREADS;
@@ -1607,7 +2066,7 @@
         retval = dfltobj;
     }
     else if ((err == DB_NOTFOUND || err == DB_KEYEMPTY)
-	     && self->moduleFlags.getReturnsNone) {
+             && self->moduleFlags.getReturnsNone) {
         err = 0;
         Py_INCREF(Py_None);
         retval = Py_None;
@@ -1615,40 +2074,32 @@
     else if (!err) {
         PyObject *pkeyObj;
         PyObject *dataObj;
-        dataObj = PyString_FromStringAndSize(data.data, data.size);
+        dataObj = Build_PyString(data.data, data.size);
 
         if (self->primaryDBType == DB_RECNO ||
             self->primaryDBType == DB_QUEUE)
-            pkeyObj = PyInt_FromLong(*(int *)pkey.data);
+            pkeyObj = NUMBER_FromLong(*(int *)pkey.data);
         else
-            pkeyObj = PyString_FromStringAndSize(pkey.data, pkey.size);
+            pkeyObj = Build_PyString(pkey.data, pkey.size);
 
         if (flags & DB_SET_RECNO) /* return key , pkey and data */
         {
             PyObject *keyObj;
             int type = _DB_get_type(self);
             if (type == DB_RECNO || type == DB_QUEUE)
-                keyObj = PyInt_FromLong(*(int *)key.data);
+                keyObj = NUMBER_FromLong(*(int *)key.data);
             else
-                keyObj = PyString_FromStringAndSize(key.data, key.size);
-#if (PY_VERSION_HEX >= 0x02040000)
+                keyObj = Build_PyString(key.data, key.size);
             retval = PyTuple_Pack(3, keyObj, pkeyObj, dataObj);
-#else
-            retval = Py_BuildValue("OOO", keyObj, pkeyObj, dataObj);
-#endif
             Py_DECREF(keyObj);
         }
         else /* return just the pkey and data */
         {
-#if (PY_VERSION_HEX >= 0x02040000)
             retval = PyTuple_Pack(2, pkeyObj, dataObj);
-#else
-            retval = Py_BuildValue("OO", pkeyObj, dataObj);
-#endif
         }
         Py_DECREF(dataObj);
         Py_DECREF(pkeyObj);
-	FREE_DBT(pkey);
+        FREE_DBT(pkey);
         FREE_DBT(data);
     }
     FREE_DBT(key);
@@ -1656,7 +2107,6 @@
     RETURN_IF_ERR();
     return retval;
 }
-#endif
 
 
 /* Return size of entry */
@@ -1690,8 +2140,8 @@
     MYDB_BEGIN_ALLOW_THREADS;
     err = self->db->get(self->db, txn, &key, &data, flags);
     MYDB_END_ALLOW_THREADS;
-    if (err == DB_BUFFER_SMALL) {
-        retval = PyInt_FromLong((long)data.size);
+    if ((err == DB_BUFFER_SMALL) || (err == 0)) {
+        retval = NUMBER_FromLong((long)data.size);
         err = 0;
     }
 
@@ -1715,7 +2165,6 @@
     DB_TXN *txn = NULL;
     static char* kwnames[] = { "key", "data", "txn", "flags", NULL };
 
-
     if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|Oi:get_both", kwnames,
                                      &keyobj, &dataobj, &txnobj, &flags))
         return NULL;
@@ -1734,7 +2183,7 @@
     orig_data = data.data;
 
     if (CHECK_DBFLAG(self, DB_THREAD)) {
-        /* Tell BerkeleyDB to malloc the return value (thread safe) */
+        /* Tell Berkeley DB to malloc the return value (thread safe) */
         /* XXX(nnorwitz): At least 4.4.20 and 4.5.20 require this flag. */
         data.flags = DB_DBT_MALLOC;
     }
@@ -1744,14 +2193,14 @@
     MYDB_END_ALLOW_THREADS;
 
     if ((err == DB_NOTFOUND || err == DB_KEYEMPTY)
-	    && self->moduleFlags.getReturnsNone) {
+            && self->moduleFlags.getReturnsNone) {
         err = 0;
         Py_INCREF(Py_None);
         retval = Py_None;
     }
     else if (!err) {
         /* XXX(nnorwitz): can we do: retval = dataobj; Py_INCREF(retval); */
-        retval = PyString_FromStringAndSize((char*)data.data, data.size);
+        retval = Build_PyString(data.data, data.size);
 
         /* Even though the flags require DB_DBT_MALLOC, data is not always
            allocated.  4.4: allocated, 4.5: *not* allocated. :-( */
@@ -1766,44 +2215,32 @@
 
 
 static PyObject*
-DB_get_byteswapped(DBObject* self, PyObject* args)
+DB_get_byteswapped(DBObject* self)
 {
-#if (DBVER >= 33)
     int err = 0;
-#endif
     int retval = -1;
 
-    if (!PyArg_ParseTuple(args,":get_byteswapped"))
-        return NULL;
     CHECK_DB_NOT_CLOSED(self);
 
-#if (DBVER >= 33)
     MYDB_BEGIN_ALLOW_THREADS;
     err = self->db->get_byteswapped(self->db, &retval);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
-#else
-    MYDB_BEGIN_ALLOW_THREADS;
-    retval = self->db->get_byteswapped(self->db);
-    MYDB_END_ALLOW_THREADS;
-#endif
-    return PyInt_FromLong(retval);
+    return NUMBER_FromLong(retval);
 }
 
 
 static PyObject*
-DB_get_type(DBObject* self, PyObject* args)
+DB_get_type(DBObject* self)
 {
     int type;
 
-    if (!PyArg_ParseTuple(args,":get_type"))
-        return NULL;
     CHECK_DB_NOT_CLOSED(self);
 
     type = _DB_get_type(self);
     if (type == -1)
         return NULL;
-    return PyInt_FromLong(type);
+    return NUMBER_FromLong(type);
 }
 
 
@@ -1830,8 +2267,8 @@
     length = PyObject_Length(cursorsObj);
     cursors = malloc((length+1) * sizeof(DBC*));
     if (!cursors) {
-	PyErr_NoMemory();
-	return NULL;
+        PyErr_NoMemory();
+        return NULL;
     }
 
     cursors[length] = NULL;
@@ -1862,7 +2299,7 @@
        but does not hold python references to them or prevent
        them from being closed prematurely.  This can cause
        python to crash when things are done in the wrong order. */
-    return (PyObject*) newDBCursorObject(dbc, self);
+    return (PyObject*) newDBCursorObject(dbc, NULL, self);
 }
 
 
@@ -1902,7 +2339,6 @@
     int err, type = DB_UNKNOWN, flags=0, mode=0660;
     char* filename = NULL;
     char* dbname = NULL;
-#if (DBVER >= 41)
     PyObject *txnobj = NULL;
     DB_TXN *txn = NULL;
     /* with dbname */
@@ -1911,83 +2347,56 @@
     /* without dbname */
     static char* kwnames_basic[] = {
         "filename", "dbtype", "flags", "mode", "txn", NULL};
-#else
-    /* with dbname */
-    static char* kwnames[] = {
-        "filename", "dbname", "dbtype", "flags", "mode", NULL};
-    /* without dbname */
-    static char* kwnames_basic[] = {
-        "filename", "dbtype", "flags", "mode", NULL};
-#endif
 
-#if (DBVER >= 41)
     if (!PyArg_ParseTupleAndKeywords(args, kwargs, "z|ziiiO:open", kwnames,
-				     &filename, &dbname, &type, &flags, &mode,
+                                     &filename, &dbname, &type, &flags, &mode,
                                      &txnobj))
-#else
-    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "z|ziii:open", kwnames,
-				     &filename, &dbname, &type, &flags,
-                                     &mode))
-#endif
     {
-	PyErr_Clear();
-	type = DB_UNKNOWN; flags = 0; mode = 0660;
-	filename = NULL; dbname = NULL;
-#if (DBVER >= 41)
-	if (!PyArg_ParseTupleAndKeywords(args, kwargs,"z|iiiO:open",
+        PyErr_Clear();
+        type = DB_UNKNOWN; flags = 0; mode = 0660;
+        filename = NULL; dbname = NULL;
+        if (!PyArg_ParseTupleAndKeywords(args, kwargs,"z|iiiO:open",
                                          kwnames_basic,
-					 &filename, &type, &flags, &mode,
+                                         &filename, &type, &flags, &mode,
                                          &txnobj))
-	    return NULL;
-#else
-	if (!PyArg_ParseTupleAndKeywords(args, kwargs,"z|iii:open",
-                                         kwnames_basic,
-					 &filename, &type, &flags, &mode))
-	    return NULL;
-#endif
+            return NULL;
     }
 
-#if (DBVER >= 41)
     if (!checkTxnObj(txnobj, &txn)) return NULL;
-#endif
 
     if (NULL == self->db) {
         PyObject *t = Py_BuildValue("(is)", 0,
                                 "Cannot call open() twice for DB object");
-        PyErr_SetObject(DBError, t);
-        Py_DECREF(t);
+        if (t) {
+            PyErr_SetObject(DBError, t);
+            Py_DECREF(t);
+        }
         return NULL;
     }
 
-#if 0 && (DBVER >= 41)
-    if ((!txn) && (txnobj != Py_None) && self->myenvobj
-        && (self->myenvobj->flags & DB_INIT_TXN))
-    {
-	/* If no 'txn' parameter was supplied (no DbTxn object and None was not
-	 * explicitly passed) but we are in a transaction ready environment:
-	 *   add DB_AUTO_COMMIT to allow for older pybsddb apps using transactions
-	 *   to work on BerkeleyDB 4.1 without needing to modify their
-	 *   DBEnv or DB open calls. 
-	 * TODO make this behaviour of the library configurable.
-	 */
-	flags |= DB_AUTO_COMMIT;
+    if (txn) {  /* Can't use 'txnobj' because could be 'txnobj==Py_None' */
+        INSERT_IN_DOUBLE_LINKED_LIST_TXN(((DBTxnObject *)txnobj)->children_dbs,self);
+        self->txn=(DBTxnObject *)txnobj;
+    } else {
+        self->txn=NULL;
     }
-#endif
 
     MYDB_BEGIN_ALLOW_THREADS;
-#if (DBVER >= 41)
     err = self->db->open(self->db, txn, filename, dbname, type, flags, mode);
-#else
-    err = self->db->open(self->db, filename, dbname, type, flags, mode);
-#endif
     MYDB_END_ALLOW_THREADS;
+
     if (makeDBError(err)) {
-        self->db->close(self->db, 0);
-        self->db = NULL;
+        PyObject *dummy;
+
+        dummy=DB_close_internal(self, 0, 0);
+        Py_XDECREF(dummy);
         return NULL;
     }
 
+    self->db->get_flags(self->db, &self->setflags);
+
     self->flags = flags;
+
     RETURN_NONE();
 }
 
@@ -2026,7 +2435,7 @@
     }
 
     if (flags & DB_APPEND)
-        retval = PyInt_FromLong(*((db_recno_t*)key.data));
+        retval = NUMBER_FromLong(*((db_recno_t*)key.data));
     else {
         retval = Py_None;
         Py_INCREF(retval);
@@ -2050,7 +2459,12 @@
         return NULL;
     CHECK_DB_NOT_CLOSED(self);
 
+    EXTRACT_FROM_DOUBLE_LINKED_LIST_MAYBE_NULL(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
     err = self->db->remove(self->db, filename, database, flags);
+    MYDB_END_ALLOW_THREADS;
+
     self->db = NULL;
     RETURN_IF_ERR();
     RETURN_NONE();
@@ -2080,45 +2494,287 @@
 
 
 static PyObject*
-DB_set_bt_minkey(DBObject* self, PyObject* args)
+DB_get_private(DBObject* self)
 {
-    int err, minkey;
+    /* We can give out the private field even if db is closed */
+    Py_INCREF(self->private_obj);
+    return self->private_obj;
+}
+
+static PyObject*
+DB_set_private(DBObject* self, PyObject* private_obj)
+{
+    /* We can set the private field even if db is closed */
+    Py_DECREF(self->private_obj);
+    Py_INCREF(private_obj);
+    self->private_obj = private_obj;
+    RETURN_NONE();
+}
+
+#if (DBVER >= 46)
+static PyObject*
+DB_set_priority(DBObject* self, PyObject* args)
+{
+    int err, priority;
 
-    if (!PyArg_ParseTuple(args,"i:set_bt_minkey", &minkey ))
+    if (!PyArg_ParseTuple(args,"i:set_priority", &priority))
         return NULL;
     CHECK_DB_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db->set_bt_minkey(self->db, minkey);
+    err = self->db->set_priority(self->db, priority);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
     RETURN_NONE();
 }
 
-#if (DBVER >= 33)
-static int 
-_default_cmp(const DBT *leftKey,
-	     const DBT *rightKey)
+static PyObject*
+DB_get_priority(DBObject* self)
 {
-  int res;
-  int lsize = leftKey->size, rsize = rightKey->size;
+    int err = 0;
+    DB_CACHE_PRIORITY priority;
 
-  res = memcmp(leftKey->data, rightKey->data, 
-	       lsize < rsize ? lsize : rsize);
-  
-  if (res == 0) {
-      if (lsize < rsize) {
-	  res = -1;
-      }
-      else if (lsize > rsize) {
-	  res = 1;
-      }
-  }
-  return res;
+    CHECK_DB_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->get_priority(self->db, &priority);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(priority);
 }
+#endif
 
-static int
-_db_compareCallback(DB* db, 
+static PyObject*
+DB_get_dbname(DBObject* self)
+{
+    int err;
+    const char *filename, *dbname;
+
+    CHECK_DB_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->get_dbname(self->db, &filename, &dbname);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    /* If "dbname==NULL", it is correctly converted to "None" */
+    return Py_BuildValue("(ss)", filename, dbname);
+}
+
+static PyObject*
+DB_get_open_flags(DBObject* self)
+{
+    int err;
+    unsigned int flags;
+
+    CHECK_DB_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->get_open_flags(self->db, &flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(flags);
+}
+
+static PyObject*
+DB_set_q_extentsize(DBObject* self, PyObject* args)
+{
+    int err;
+    u_int32_t extentsize;
+
+    if (!PyArg_ParseTuple(args,"i:set_q_extentsize", &extentsize))
+        return NULL;
+    CHECK_DB_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->set_q_extentsize(self->db, extentsize);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DB_get_q_extentsize(DBObject* self)
+{
+    int err = 0;
+    u_int32_t extentsize;
+
+    CHECK_DB_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->get_q_extentsize(self->db, &extentsize);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(extentsize);
+}
+
+static PyObject*
+DB_set_bt_minkey(DBObject* self, PyObject* args)
+{
+    int err, minkey;
+
+    if (!PyArg_ParseTuple(args,"i:set_bt_minkey", &minkey))
+        return NULL;
+    CHECK_DB_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->set_bt_minkey(self->db, minkey);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DB_get_bt_minkey(DBObject* self)
+{
+    int err;
+    u_int32_t bt_minkey;
+
+    CHECK_DB_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->get_bt_minkey(self->db, &bt_minkey);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(bt_minkey);
+}
+
+static int
+_default_cmp(const DBT *leftKey,
+             const DBT *rightKey)
+{
+  int res;
+  int lsize = leftKey->size, rsize = rightKey->size;
+
+  res = memcmp(leftKey->data, rightKey->data,
+               lsize < rsize ? lsize : rsize);
+
+  if (res == 0) {
+      if (lsize < rsize) {
+          res = -1;
+      }
+      else if (lsize > rsize) {
+          res = 1;
+      }
+  }
+  return res;
+}
+
+static int
+_db_compareCallback(DB* db,
+                    const DBT *leftKey,
+                    const DBT *rightKey)
+{
+    int res = 0;
+    PyObject *args;
+    PyObject *result = NULL;
+    DBObject *self = (DBObject *)db->app_private;
+
+    if (self == NULL || self->btCompareCallback == NULL) {
+        MYDB_BEGIN_BLOCK_THREADS;
+        PyErr_SetString(PyExc_TypeError,
+                        (self == 0
+                         ? "DB_bt_compare db is NULL."
+                         : "DB_bt_compare callback is NULL."));
+        /* we're in a callback within the DB code, we can't raise */
+        PyErr_Print();
+        res = _default_cmp(leftKey, rightKey);
+        MYDB_END_BLOCK_THREADS;
+    } else {
+        MYDB_BEGIN_BLOCK_THREADS;
+
+        args = BuildValue_SS(leftKey->data, leftKey->size, rightKey->data, rightKey->size);
+        if (args != NULL) {
+                result = PyEval_CallObject(self->btCompareCallback, args);
+        }
+        if (args == NULL || result == NULL) {
+            /* we're in a callback within the DB code, we can't raise */
+            PyErr_Print();
+            res = _default_cmp(leftKey, rightKey);
+        } else if (NUMBER_Check(result)) {
+            res = NUMBER_AsLong(result);
+        } else {
+            PyErr_SetString(PyExc_TypeError,
+                            "DB_bt_compare callback MUST return an int.");
+            /* we're in a callback within the DB code, we can't raise */
+            PyErr_Print();
+            res = _default_cmp(leftKey, rightKey);
+        }
+
+        Py_XDECREF(args);
+        Py_XDECREF(result);
+
+        MYDB_END_BLOCK_THREADS;
+    }
+    return res;
+}
+
+static PyObject*
+DB_set_bt_compare(DBObject* self, PyObject* comparator)
+{
+    int err;
+    PyObject *tuple, *result;
+
+    CHECK_DB_NOT_CLOSED(self);
+
+    if (!PyCallable_Check(comparator)) {
+        makeTypeError("Callable", comparator);
+        return NULL;
+    }
+
+    /*
+     * Perform a test call of the comparator function with two empty
+     * string objects here.  verify that it returns an int (0).
+     * err if not.
+     */
+    tuple = Py_BuildValue("(ss)", "", "");
+    result = PyEval_CallObject(comparator, tuple);
+    Py_DECREF(tuple);
+    if (result == NULL)
+        return NULL;
+    if (!NUMBER_Check(result)) {
+        Py_DECREF(result);
+        PyErr_SetString(PyExc_TypeError,
+                        "callback MUST return an int");
+        return NULL;
+    } else if (NUMBER_AsLong(result) != 0) {
+        Py_DECREF(result);
+        PyErr_SetString(PyExc_TypeError,
+                        "callback failed to return 0 on two empty strings");
+        return NULL;
+    }
+    Py_DECREF(result);
+
+    /* We don't accept multiple set_bt_compare operations, in order to
+     * simplify the code. This would have no real use, as one cannot
+     * change the function once the db is opened anyway */
+    if (self->btCompareCallback != NULL) {
+        PyErr_SetString(PyExc_RuntimeError, "set_bt_compare() cannot be called more than once");
+        return NULL;
+    }
+
+    Py_INCREF(comparator);
+    self->btCompareCallback = comparator;
+
+    /* This is to workaround a problem with un-initialized threads (see
+       comment in DB_associate) */
+#ifdef WITH_THREAD
+    PyEval_InitThreads();
+#endif
+
+    err = self->db->set_bt_compare(self->db, _db_compareCallback);
+
+    if (err) {
+        /* restore the old state in case of error */
+        Py_DECREF(comparator);
+        self->btCompareCallback = NULL;
+    }
+
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static int
+_db_dupCompareCallback(DB* db,
 		    const DBT *leftKey,
 		    const DBT *rightKey)
 {
@@ -2127,12 +2783,12 @@
     PyObject *result = NULL;
     DBObject *self = (DBObject *)db->app_private;
 
-    if (self == NULL || self->btCompareCallback == NULL) {
+    if (self == NULL || self->dupCompareCallback == NULL) {
 	MYDB_BEGIN_BLOCK_THREADS;
 	PyErr_SetString(PyExc_TypeError,
 			(self == 0
-			 ? "DB_bt_compare db is NULL."
-			 : "DB_bt_compare callback is NULL."));
+			 ? "DB_dup_compare db is NULL."
+			 : "DB_dup_compare callback is NULL."));
 	/* we're in a callback within the DB code, we can't raise */
 	PyErr_Print();
 	res = _default_cmp(leftKey, rightKey);
@@ -2140,27 +2796,24 @@
     } else {
 	MYDB_BEGIN_BLOCK_THREADS;
 
-	args = Py_BuildValue("s#s#", leftKey->data, leftKey->size,
-			     rightKey->data, rightKey->size);
+	args = BuildValue_SS(leftKey->data, leftKey->size, rightKey->data, rightKey->size);
 	if (args != NULL) {
-		/* XXX(twouters) I highly doubt this INCREF is correct */
-		Py_INCREF(self);
-		result = PyEval_CallObject(self->btCompareCallback, args);
+		result = PyEval_CallObject(self->dupCompareCallback, args);
 	}
 	if (args == NULL || result == NULL) {
 	    /* we're in a callback within the DB code, we can't raise */
 	    PyErr_Print();
 	    res = _default_cmp(leftKey, rightKey);
-	} else if (PyInt_Check(result)) {
-	    res = PyInt_AsLong(result);
+	} else if (NUMBER_Check(result)) {
+	    res = NUMBER_AsLong(result);
 	} else {
 	    PyErr_SetString(PyExc_TypeError,
-			    "DB_bt_compare callback MUST return an int.");
+			    "DB_dup_compare callback MUST return an int.");
 	    /* we're in a callback within the DB code, we can't raise */
 	    PyErr_Print();
 	    res = _default_cmp(leftKey, rightKey);
 	}
-    
+
 	Py_XDECREF(args);
 	Py_XDECREF(result);
 
@@ -2170,15 +2823,11 @@
 }
 
 static PyObject*
-DB_set_bt_compare(DBObject* self, PyObject* args)
+DB_set_dup_compare(DBObject* self, PyObject* comparator)
 {
     int err;
-    PyObject *comparator;
     PyObject *tuple, *result;
 
-    if (!PyArg_ParseTuple(args, "O:set_bt_compare", &comparator))
-	return NULL;
-
     CHECK_DB_NOT_CLOSED(self);
 
     if (!PyCallable_Check(comparator)) {
@@ -2186,7 +2835,7 @@
 	return NULL;
     }
 
-    /* 
+    /*
      * Perform a test call of the comparator function with two empty
      * string objects here.  verify that it returns an int (0).
      * err if not.
@@ -2196,27 +2845,29 @@
     Py_DECREF(tuple);
     if (result == NULL)
         return NULL;
-    if (!PyInt_Check(result)) {
+    if (!NUMBER_Check(result)) {
+	Py_DECREF(result);
 	PyErr_SetString(PyExc_TypeError,
 		        "callback MUST return an int");
 	return NULL;
-    } else if (PyInt_AsLong(result) != 0) {
+    } else if (NUMBER_AsLong(result) != 0) {
+	Py_DECREF(result);
 	PyErr_SetString(PyExc_TypeError,
 		        "callback failed to return 0 on two empty strings");
 	return NULL;
     }
     Py_DECREF(result);
 
-    /* We don't accept multiple set_bt_compare operations, in order to
+    /* We don't accept multiple set_dup_compare operations, in order to
      * simplify the code. This would have no real use, as one cannot
      * change the function once the db is opened anyway */
-    if (self->btCompareCallback != NULL) {
-	PyErr_SetString(PyExc_RuntimeError, "set_bt_compare() cannot be called more than once");
+    if (self->dupCompareCallback != NULL) {
+	PyErr_SetString(PyExc_RuntimeError, "set_dup_compare() cannot be called more than once");
 	return NULL;
     }
 
     Py_INCREF(comparator);
-    self->btCompareCallback = comparator;
+    self->dupCompareCallback = comparator;
 
     /* This is to workaround a problem with un-initialized threads (see
        comment in DB_associate) */
@@ -2224,18 +2875,17 @@
     PyEval_InitThreads();
 #endif
 
-    err = self->db->set_bt_compare(self->db, _db_compareCallback);
+    err = self->db->set_dup_compare(self->db, _db_dupCompareCallback);
 
     if (err) {
 	/* restore the old state in case of error */
 	Py_DECREF(comparator);
-	self->btCompareCallback = NULL;
+	self->dupCompareCallback = NULL;
     }
 
     RETURN_IF_ERR();
     RETURN_NONE();
 }
-#endif /* DBVER >= 33 */
 
 
 static PyObject*
@@ -2256,6 +2906,23 @@
     RETURN_NONE();
 }
 
+static PyObject*
+DB_get_cachesize(DBObject* self)
+{
+    int err;
+    u_int32_t gbytes, bytes;
+    int ncache;
+
+    CHECK_DB_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->get_cachesize(self->db, &gbytes, &bytes, &ncache);
+    MYDB_END_ALLOW_THREADS;
+
+    RETURN_IF_ERR();
+
+    return Py_BuildValue("(iii)", gbytes, bytes, ncache);
+}
 
 static PyObject*
 DB_set_flags(DBObject* self, PyObject* args)
@@ -2275,6 +2942,48 @@
     RETURN_NONE();
 }
 
+static PyObject*
+DB_get_flags(DBObject* self)
+{
+    int err;
+    u_int32_t flags;
+
+    CHECK_DB_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->get_flags(self->db, &flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(flags);
+}
+
+static PyObject*
+DB_get_transactional(DBObject* self)
+{
+    int err;
+
+    CHECK_DB_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->get_transactional(self->db);
+    MYDB_END_ALLOW_THREADS;
+
+    if(err == 0) {
+        Py_INCREF(Py_False);
+        return Py_False;
+    } else if(err == 1) {
+        Py_INCREF(Py_True);
+        return Py_True;
+    }
+
+    /*
+    ** If we reach there, there was an error. The
+    ** "return" should be unreachable.
+    */
+    RETURN_IF_ERR();
+    assert(0);  /* This code SHOULD be unreachable */
+    return NULL;
+}
 
 static PyObject*
 DB_set_h_ffactor(DBObject* self, PyObject* args)
@@ -2292,6 +3001,20 @@
     RETURN_NONE();
 }
 
+static PyObject*
+DB_get_h_ffactor(DBObject* self)
+{
+    int err;
+    u_int32_t ffactor;
+
+    CHECK_DB_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->get_h_ffactor(self->db, &ffactor);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(ffactor);
+}
 
 static PyObject*
 DB_set_h_nelem(DBObject* self, PyObject* args)
@@ -2309,6 +3032,20 @@
     RETURN_NONE();
 }
 
+static PyObject*
+DB_get_h_nelem(DBObject* self)
+{
+    int err;
+    u_int32_t nelem;
+
+    CHECK_DB_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->get_h_nelem(self->db, &nelem);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(nelem);
+}
 
 static PyObject*
 DB_set_lorder(DBObject* self, PyObject* args)
@@ -2326,6 +3063,20 @@
     RETURN_NONE();
 }
 
+static PyObject*
+DB_get_lorder(DBObject* self)
+{
+    int err;
+    int lorder;
+
+    CHECK_DB_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->get_lorder(self->db, &lorder);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(lorder);
+}
 
 static PyObject*
 DB_set_pagesize(DBObject* self, PyObject* args)
@@ -2343,12 +3094,26 @@
     RETURN_NONE();
 }
 
-
 static PyObject*
-DB_set_re_delim(DBObject* self, PyObject* args)
+DB_get_pagesize(DBObject* self)
 {
     int err;
-    char delim;
+    u_int32_t pagesize;
+
+    CHECK_DB_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->get_pagesize(self->db, &pagesize);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(pagesize);
+}
+
+static PyObject*
+DB_set_re_delim(DBObject* self, PyObject* args)
+{
+    int err;
+    char delim;
 
     if (!PyArg_ParseTuple(args,"b:set_re_delim", &delim)) {
         PyErr_Clear();
@@ -2366,6 +3131,20 @@
 }
 
 static PyObject*
+DB_get_re_delim(DBObject* self)
+{
+    int err, re_delim;
+
+    CHECK_DB_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->get_re_delim(self->db, &re_delim);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(re_delim);
+}
+
+static PyObject*
 DB_set_re_len(DBObject* self, PyObject* args)
 {
     int err, len;
@@ -2381,6 +3160,20 @@
     RETURN_NONE();
 }
 
+static PyObject*
+DB_get_re_len(DBObject* self)
+{
+    int err;
+    u_int32_t re_len;
+
+    CHECK_DB_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->get_re_len(self->db, &re_len);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(re_len);
+}
 
 static PyObject*
 DB_set_re_pad(DBObject* self, PyObject* args)
@@ -2402,43 +3195,51 @@
     RETURN_NONE();
 }
 
+static PyObject*
+DB_get_re_pad(DBObject* self)
+{
+    int err, re_pad;
+
+    CHECK_DB_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->get_re_pad(self->db, &re_pad);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(re_pad);
+}
 
 static PyObject*
 DB_set_re_source(DBObject* self, PyObject* args)
 {
     int err;
-    char *re_source;
+    char *source;
 
-    if (!PyArg_ParseTuple(args,"s:set_re_source", &re_source))
+    if (!PyArg_ParseTuple(args,"s:set_re_source", &source))
         return NULL;
     CHECK_DB_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db->set_re_source(self->db, re_source);
+    err = self->db->set_re_source(self->db, source);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
     RETURN_NONE();
 }
 
-
-#if (DBVER >= 32)
 static PyObject*
-DB_set_q_extentsize(DBObject* self, PyObject* args)
+DB_get_re_source(DBObject* self)
 {
     int err;
-    int extentsize;
+    const char *source;
 
-    if (!PyArg_ParseTuple(args,"i:set_q_extentsize", &extentsize))
-        return NULL;
     CHECK_DB_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db->set_q_extentsize(self->db, extentsize);
+    err = self->db->get_re_source(self->db, &source);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
-    RETURN_NONE();
+    return PyBytes_FromString(source);
 }
-#endif
 
 static PyObject*
 DB_stat(DBObject* self, PyObject* args, PyObject* kwargs)
@@ -2446,39 +3247,22 @@
     int err, flags = 0, type;
     void* sp;
     PyObject* d;
-#if (DBVER >= 43)
     PyObject* txnobj = NULL;
     DB_TXN *txn = NULL;
     static char* kwnames[] = { "flags", "txn", NULL };
-#else
-    static char* kwnames[] = { "flags", NULL };
-#endif
 
-#if (DBVER >= 43)
     if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|iO:stat", kwnames,
                                      &flags, &txnobj))
         return NULL;
     if (!checkTxnObj(txnobj, &txn))
         return NULL;
-#else
-    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i:stat", kwnames, &flags))
-        return NULL;
-#endif
     CHECK_DB_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-#if (DBVER >= 43)
     err = self->db->stat(self->db, txn, &sp, flags);
-#elif (DBVER >= 33)
-    err = self->db->stat(self->db, &sp, flags);
-#else
-    err = self->db->stat(self->db, &sp, NULL, flags);
-#endif
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
 
-    self->haveStat = 1;
-
     /* Turn the stat structure into a dictionary */
     type = _DB_get_type(self);
     if ((type == -1) || ((d = PyDict_New()) == NULL)) {
@@ -2496,10 +3280,10 @@
         MAKE_HASH_ENTRY(version);
         MAKE_HASH_ENTRY(nkeys);
         MAKE_HASH_ENTRY(ndata);
-        MAKE_HASH_ENTRY(pagesize);
-#if (DBVER < 41)
-        MAKE_HASH_ENTRY(nelem);
+#if (DBVER >= 46)
+        MAKE_HASH_ENTRY(pagecnt);
 #endif
+        MAKE_HASH_ENTRY(pagesize);
         MAKE_HASH_ENTRY(ffactor);
         MAKE_HASH_ENTRY(buckets);
         MAKE_HASH_ENTRY(free);
@@ -2518,6 +3302,9 @@
         MAKE_BT_ENTRY(version);
         MAKE_BT_ENTRY(nkeys);
         MAKE_BT_ENTRY(ndata);
+#if (DBVER >= 46)
+        MAKE_BT_ENTRY(pagecnt);
+#endif
         MAKE_BT_ENTRY(pagesize);
         MAKE_BT_ENTRY(minkey);
         MAKE_BT_ENTRY(re_len);
@@ -2527,6 +3314,7 @@
         MAKE_BT_ENTRY(leaf_pg);
         MAKE_BT_ENTRY(dup_pg);
         MAKE_BT_ENTRY(over_pg);
+        MAKE_BT_ENTRY(empty_pg);
         MAKE_BT_ENTRY(free);
         MAKE_BT_ENTRY(int_pgfree);
         MAKE_BT_ENTRY(leaf_pgfree);
@@ -2540,6 +3328,7 @@
         MAKE_QUEUE_ENTRY(nkeys);
         MAKE_QUEUE_ENTRY(ndata);
         MAKE_QUEUE_ENTRY(pagesize);
+        MAKE_QUEUE_ENTRY(extentsize);
         MAKE_QUEUE_ENTRY(pages);
         MAKE_QUEUE_ENTRY(re_len);
         MAKE_QUEUE_ENTRY(re_pad);
@@ -2566,6 +3355,27 @@
 }
 
 static PyObject*
+DB_stat_print(DBObject* self, PyObject* args, PyObject *kwargs)
+{
+    int err;
+    int flags=0;
+    static char* kwnames[] = { "flags", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i:stat_print",
+                kwnames, &flags))
+    {
+        return NULL;
+    }
+    CHECK_DB_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->stat_print(self->db, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+
+static PyObject*
 DB_sync(DBObject* self, PyObject* args)
 {
     int err;
@@ -2583,7 +3393,6 @@
 }
 
 
-#if (DBVER >= 33)
 static PyObject*
 DB_truncate(DBObject* self, PyObject* args, PyObject* kwargs)
 {
@@ -2604,9 +3413,8 @@
     err = self->db->truncate(self->db, txn, &count, flags);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
-    return PyInt_FromLong(count);
+    return NUMBER_FromLong(count);
 }
-#endif
 
 
 static PyObject*
@@ -2645,25 +3453,27 @@
     CHECK_DB_NOT_CLOSED(self);
     if (outFileName)
         outFile = fopen(outFileName, "w");
-	/* XXX(nnorwitz): it should probably be an exception if outFile
-	   can't be opened. */
+        /* XXX(nnorwitz): it should probably be an exception if outFile
+           can't be opened. */
+
+    {  /* DB.verify acts as a DB handle destructor (like close) */
+        PyObject *error;
+
+        error=DB_close_internal(self, 0, 1);
+        if (error) {
+          return error;
+        }
+     }
 
     MYDB_BEGIN_ALLOW_THREADS;
     err = self->db->verify(self->db, fileName, dbName, outFile, flags);
     MYDB_END_ALLOW_THREADS;
+
+    self->db = NULL;  /* Implicit close; related objects already released */
+
     if (outFile)
         fclose(outFile);
 
-    /* DB.verify acts as a DB handle destructor (like close); this was
-     * documented in BerkeleyDB 4.2 but had the undocumented effect
-     * of not being safe in prior versions while still requiring an explicit
-     * DB.close call afterwards.  Lets call close for the user to emulate
-     * the safe 4.2 behaviour. */
-#if (DBVER <= 41)
-    self->db->close(self->db, 0);
-#endif
-    self->db = NULL;
-
     RETURN_IF_ERR();
     RETURN_NONE();
 }
@@ -2685,10 +3495,9 @@
         ++oldValue;
     self->moduleFlags.getReturnsNone = (flags >= 1);
     self->moduleFlags.cursorSetReturnsNone = (flags >= 2);
-    return PyInt_FromLong(oldValue);
+    return NUMBER_FromLong(oldValue);
 }
 
-#if (DBVER >= 41)
 static PyObject*
 DB_set_encrypt(DBObject* self, PyObject* args, PyObject* kwargs)
 {
@@ -2698,8 +3507,8 @@
     static char* kwnames[] = { "passwd", "flags", NULL };
 
     if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|i:set_encrypt", kwnames,
-		&passwd, &flags)) {
-	return NULL;
+                &passwd, &flags)) {
+        return NULL;
     }
 
     MYDB_BEGIN_ALLOW_THREADS;
@@ -2709,7 +3518,22 @@
     RETURN_IF_ERR();
     RETURN_NONE();
 }
-#endif /* DBVER >= 41 */
+
+static PyObject*
+DB_get_encrypt_flags(DBObject* self)
+{
+    int err;
+    u_int32_t flags;
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db->get_encrypt_flags(self->db, &flags);
+    MYDB_END_ALLOW_THREADS;
+
+    RETURN_IF_ERR();
+
+    return NUMBER_FromLong(flags);
+}
+
 
 
 /*-------------------------------------------------------------- */
@@ -2719,54 +3543,29 @@
 {
     int err;
     Py_ssize_t size = 0;
-    int flags = 0;
     void* sp;
     DBObject* self = (DBObject*)_self;
 
     if (self->db == NULL) {
         PyObject *t = Py_BuildValue("(is)", 0, "DB object has been closed");
-        PyErr_SetObject(DBError, t);
-        Py_DECREF(t);
+        if (t) {
+            PyErr_SetObject(DBError, t);
+            Py_DECREF(t);
+        }
         return -1;
     }
 
-    if (self->haveStat) {  /* Has the stat function been called recently?  If
-                              so, we can use the cached value. */
-        flags = DB_FAST_STAT;
-    }
-
     MYDB_BEGIN_ALLOW_THREADS;
-redo_stat_for_length:
-#if (DBVER >= 43)
-    err = self->db->stat(self->db, /*txnid*/ NULL, &sp, flags);
-#elif (DBVER >= 33)
-    err = self->db->stat(self->db, &sp, flags);
-#else
-    err = self->db->stat(self->db, &sp, NULL, flags);
-#endif
+    err = self->db->stat(self->db, /*txnid*/ NULL, &sp, 0);
+    MYDB_END_ALLOW_THREADS;
 
     /* All the stat structures have matching fields upto the ndata field,
        so we can use any of them for the type cast */
     size = ((DB_BTREE_STAT*)sp)->bt_ndata;
 
-    /* A size of 0 could mean that BerkeleyDB no longer had the stat values cached.
-     * redo a full stat to make sure.
-     *   Fixes SF python bug 1493322, pybsddb bug 1184012
-     */
-    if (size == 0 && (flags & DB_FAST_STAT)) {
-        flags = 0;
-        if (!err)
-            free(sp);
-        goto redo_stat_for_length;
-    }
-
-    MYDB_END_ALLOW_THREADS;
-
     if (err)
         return -1;
 
-    self->haveStat = 1;
-
     free(sp);
     return size;
 }
@@ -2785,7 +3584,7 @@
 
     CLEAR_DBT(data);
     if (CHECK_DBFLAG(self, DB_THREAD)) {
-        /* Tell BerkeleyDB to malloc the return value (thread safe) */
+        /* Tell Berkeley DB to malloc the return value (thread safe) */
         data.flags = DB_DBT_MALLOC;
     }
     MYDB_BEGIN_ALLOW_THREADS;
@@ -2799,7 +3598,7 @@
         retval = NULL;
     }
     else {
-        retval = PyString_FromStringAndSize((char*)data.data, data.size);
+        retval = Build_PyString(data.data, data.size);
         FREE_DBT(data);
     }
 
@@ -2817,8 +3616,10 @@
 
     if (self->db == NULL) {
         PyObject *t = Py_BuildValue("(is)", 0, "DB object has been closed");
-        PyErr_SetObject(DBError, t);
-        Py_DECREF(t);
+        if (t) {
+            PyErr_SetObject(DBError, t);
+            Py_DECREF(t);
+        }
         return -1;
     }
 
@@ -2853,16 +3654,12 @@
 
 
 static PyObject*
-DB_has_key(DBObject* self, PyObject* args)
+_DB_has_key(DBObject* self, PyObject* keyobj, PyObject* txnobj)
 {
     int err;
-    PyObject* keyobj;
-    DBT key, data;
-    PyObject* txnobj = NULL;
+    DBT key;
     DB_TXN *txn = NULL;
 
-    if (!PyArg_ParseTuple(args,"O|O:has_key", &keyobj, &txnobj))
-        return NULL;
     CHECK_DB_NOT_CLOSED(self);
     if (!make_key_dbt(self, keyobj, &key, NULL))
         return NULL;
@@ -2871,28 +3668,77 @@
         return NULL;
     }
 
+#if (DBVER < 46)
     /* This causes DB_BUFFER_SMALL to be returned when the db has the key because
        it has a record but can't allocate a buffer for the data.  This saves
        having to deal with data we won't be using.
      */
-    CLEAR_DBT(data);
-    data.flags = DB_DBT_USERMEM;
+    {
+        DBT data ;
+        CLEAR_DBT(data);
+        data.flags = DB_DBT_USERMEM;
 
+        MYDB_BEGIN_ALLOW_THREADS;
+        err = self->db->get(self->db, txn, &key, &data, 0);
+        MYDB_END_ALLOW_THREADS;
+    }
+#else
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db->get(self->db, txn, &key, &data, 0);
+    err = self->db->exists(self->db, txn, &key, 0);
     MYDB_END_ALLOW_THREADS;
+#endif
+
     FREE_DBT(key);
 
+    /*
+    ** DB_BUFFER_SMALL is only used if we use "get".
+    ** We can drop it when we only use "exists",
+    ** when we drop suport for Berkeley DB < 4.6.
+    */
     if (err == DB_BUFFER_SMALL || err == 0) {
-        return PyInt_FromLong(1);
+        Py_INCREF(Py_True);
+        return Py_True;
     } else if (err == DB_NOTFOUND || err == DB_KEYEMPTY) {
-        return PyInt_FromLong(0);
+        Py_INCREF(Py_False);
+        return Py_False;
     }
 
     makeDBError(err);
     return NULL;
 }
 
+static PyObject*
+DB_has_key(DBObject* self, PyObject* args, PyObject* kwargs)
+{
+    PyObject* keyobj;
+    PyObject* txnobj = NULL;
+    static char* kwnames[] = {"key","txn", NULL};
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|O:has_key", kwnames,
+                &keyobj, &txnobj))
+        return NULL;
+
+    return _DB_has_key(self, keyobj, txnobj);
+}
+
+
+static int DB_contains(DBObject* self, PyObject* keyobj)
+{
+    PyObject* result;
+    int result2 = 0;
+
+    result = _DB_has_key(self, keyobj, NULL) ;
+    if (result == NULL) {
+        return -1; /* Propague exception */
+    }
+    if (result != Py_False) {
+        result2 = 1;
+    }
+
+    Py_DECREF(result);
+    return result2;
+}
+
 
 #define _KEYS_LIST      1
 #define _VALUES_LIST    2
@@ -2929,14 +3775,9 @@
         return NULL;
     }
 
-    if (CHECK_DBFLAG(self, DB_THREAD)) {
-        key.flags = DB_DBT_REALLOC;
-        data.flags = DB_DBT_REALLOC;
-    }
-
     while (1) { /* use the cursor to traverse the DB, collecting items */
         MYDB_BEGIN_ALLOW_THREADS;
-        err = cursor->c_get(cursor, &key, &data, DB_NEXT);
+        err = _DBC_get(cursor, &key, &data, DB_NEXT);
         MYDB_END_ALLOW_THREADS;
 
         if (err) {
@@ -2950,17 +3791,17 @@
             case DB_BTREE:
             case DB_HASH:
             default:
-                item = PyString_FromStringAndSize((char*)key.data, key.size);
+                item = Build_PyString(key.data, key.size);
                 break;
             case DB_RECNO:
             case DB_QUEUE:
-                item = PyInt_FromLong(*((db_recno_t*)key.data));
+                item = NUMBER_FromLong(*((db_recno_t*)key.data));
                 break;
             }
             break;
 
         case _VALUES_LIST:
-            item = PyString_FromStringAndSize((char*)data.data, data.size);
+            item = Build_PyString(data.data, data.size);
             break;
 
         case _ITEMS_LIST:
@@ -2968,13 +3809,11 @@
             case DB_BTREE:
             case DB_HASH:
             default:
-                item = Py_BuildValue("s#s#", key.data, key.size, data.data,
-                                     data.size);
+                item = BuildValue_SS(key.data, key.size, data.data, data.size);
                 break;
             case DB_RECNO:
             case DB_QUEUE:
-                item = Py_BuildValue("is#", *((db_recno_t*)key.data),
-                                     data.data, data.size);
+                item = BuildValue_IS(*((db_recno_t*)key.data), data.data, data.size);
                 break;
             }
             break;
@@ -2988,7 +3827,12 @@
             list = NULL;
             goto done;
         }
-        PyList_Append(list, item);
+        if (PyList_Append(list, item)) {
+            Py_DECREF(list);
+            Py_DECREF(item);
+            list = NULL;
+            goto done;
+        }
         Py_DECREF(item);
     }
 
@@ -2999,10 +3843,8 @@
     }
 
  done:
-    FREE_DBT(key);
-    FREE_DBT(data);
     MYDB_BEGIN_ALLOW_THREADS;
-    cursor->c_close(cursor);
+    _DBC_close(cursor);
     MYDB_END_ALLOW_THREADS;
     return list;
 }
@@ -3050,110 +3892,358 @@
 }
 
 /* --------------------------------------------------------------------- */
-/* DBCursor methods */
+/* DBLogCursor methods */
 
 
 static PyObject*
-DBC_close(DBCursorObject* self, PyObject* args)
+DBLogCursor_close_internal(DBLogCursorObject* self)
 {
     int err = 0;
 
-    if (!PyArg_ParseTuple(args, ":close"))
-        return NULL;
+    if (self->logc != NULL) {
+        EXTRACT_FROM_DOUBLE_LINKED_LIST(self);
 
-    if (self->dbc != NULL) {
         MYDB_BEGIN_ALLOW_THREADS;
-        err = self->dbc->c_close(self->dbc);
-        self->dbc = NULL;
+        err = self->logc->close(self->logc, 0);
         MYDB_END_ALLOW_THREADS;
+        self->logc = NULL;
     }
     RETURN_IF_ERR();
     RETURN_NONE();
 }
 
+static PyObject*
+DBLogCursor_close(DBLogCursorObject* self)
+{
+    return DBLogCursor_close_internal(self);
+}
+
 
 static PyObject*
-DBC_count(DBCursorObject* self, PyObject* args)
+_DBLogCursor_get(DBLogCursorObject* self, int flag, DB_LSN *lsn2)
 {
-    int err = 0;
-    db_recno_t count;
-    int flags = 0;
+    int err;
+    DBT data;
+    DB_LSN lsn = {0, 0};
+    PyObject *dummy, *retval;
 
-    if (!PyArg_ParseTuple(args, "|i:count", &flags))
-        return NULL;
+    CLEAR_DBT(data);
+    data.flags = DB_DBT_MALLOC; /* Berkeley DB must do the malloc */
 
-    CHECK_CURSOR_NOT_CLOSED(self);
+    CHECK_LOGCURSOR_NOT_CLOSED(self);
+
+    if (lsn2)
+        lsn = *lsn2;
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->dbc->c_count(self->dbc, &count, flags);
+    err = self->logc->get(self->logc, &lsn, &data, flag);
     MYDB_END_ALLOW_THREADS;
-    RETURN_IF_ERR();
 
-    return PyInt_FromLong(count);
+    if (err == DB_NOTFOUND) {
+        Py_INCREF(Py_None);
+        retval = Py_None;
+    }
+    else if (makeDBError(err)) {
+        retval = NULL;
+    }
+    else {
+        retval = dummy = BuildValue_S(data.data, data.size);
+        if (dummy) {
+            retval = Py_BuildValue("(ii)O", lsn.file, lsn.offset, dummy);
+            Py_DECREF(dummy);
+        }
+    }
+
+    FREE_DBT(data);
+    return retval;
+}
+
+static PyObject*
+DBLogCursor_current(DBLogCursorObject* self)
+{
+    return _DBLogCursor_get(self, DB_CURRENT, NULL);
 }
 
+static PyObject*
+DBLogCursor_first(DBLogCursorObject* self)
+{
+    return _DBLogCursor_get(self, DB_FIRST, NULL);
+}
 
 static PyObject*
-DBC_current(DBCursorObject* self, PyObject* args, PyObject *kwargs)
+DBLogCursor_last(DBLogCursorObject* self)
 {
-    return _DBCursor_get(self,DB_CURRENT,args,kwargs,"|iii:current");
+    return _DBLogCursor_get(self, DB_LAST, NULL);
 }
 
+static PyObject*
+DBLogCursor_next(DBLogCursorObject* self)
+{
+    return _DBLogCursor_get(self, DB_NEXT, NULL);
+}
 
 static PyObject*
-DBC_delete(DBCursorObject* self, PyObject* args)
+DBLogCursor_prev(DBLogCursorObject* self)
 {
-    int err, flags=0;
+    return _DBLogCursor_get(self, DB_PREV, NULL);
+}
 
-    if (!PyArg_ParseTuple(args, "|i:delete", &flags))
+static PyObject*
+DBLogCursor_set(DBLogCursorObject* self, PyObject* args)
+{
+    DB_LSN lsn;
+
+    if (!PyArg_ParseTuple(args, "(ii):set", &lsn.file, &lsn.offset))
         return NULL;
 
-    CHECK_CURSOR_NOT_CLOSED(self);
+    return _DBLogCursor_get(self, DB_SET, &lsn);
+}
 
-    MYDB_BEGIN_ALLOW_THREADS;
-    err = self->dbc->c_del(self->dbc, flags);
-    MYDB_END_ALLOW_THREADS;
-    RETURN_IF_ERR();
 
-    self->mydb->haveStat = 0;
+/* --------------------------------------------------------------------- */
+/* DBSite methods */
+
+
+#if (DBVER >= 52)
+static PyObject*
+DBSite_close_internal(DBSiteObject* self)
+{
+    int err = 0;
+
+    if (self->site != NULL) {
+        EXTRACT_FROM_DOUBLE_LINKED_LIST(self);
+
+        MYDB_BEGIN_ALLOW_THREADS;
+        err = self->site->close(self->site);
+        MYDB_END_ALLOW_THREADS;
+        self->site = NULL;
+    }
+    RETURN_IF_ERR();
     RETURN_NONE();
 }
 
-
 static PyObject*
-DBC_dup(DBCursorObject* self, PyObject* args)
+DBSite_close(DBSiteObject* self)
 {
-    int err, flags =0;
-    DBC* dbc = NULL;
+    return DBSite_close_internal(self);
+}
 
-    if (!PyArg_ParseTuple(args, "|i:dup", &flags))
-        return NULL;
+static PyObject*
+DBSite_remove(DBSiteObject* self)
+{
+    int err = 0;
 
-    CHECK_CURSOR_NOT_CLOSED(self);
+    CHECK_SITE_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->dbc->c_dup(self->dbc, &dbc, flags);
+    err = self->site->remove(self->site);
     MYDB_END_ALLOW_THREADS;
-    RETURN_IF_ERR();
 
-    return (PyObject*) newDBCursorObject(dbc, self->mydb);
+    RETURN_IF_ERR();
+    RETURN_NONE();
 }
 
 static PyObject*
-DBC_first(DBCursorObject* self, PyObject* args, PyObject* kwargs)
+DBSite_get_eid(DBSiteObject* self)
 {
-    return _DBCursor_get(self,DB_FIRST,args,kwargs,"|iii:first");
-}
+    int err = 0;
+    int eid;
+
+    CHECK_SITE_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->site->get_eid(self->site, &eid);
+    MYDB_END_ALLOW_THREADS;
 
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(eid);
+}
 
 static PyObject*
-DBC_get(DBCursorObject* self, PyObject* args, PyObject *kwargs)
+DBSite_get_address(DBSiteObject* self)
 {
-    int err, flags=0;
-    PyObject* keyobj = NULL;
-    PyObject* dataobj = NULL;
-    PyObject* retval = NULL;
-    int dlen = -1;
+    int err = 0;
+    const char *host;
+    u_int port;
+
+    CHECK_SITE_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->site->get_address(self->site, &host, &port);
+    MYDB_END_ALLOW_THREADS;
+
+    RETURN_IF_ERR();
+
+    return Py_BuildValue("(sI)", host, port);
+}
+
+static PyObject*
+DBSite_get_config(DBSiteObject* self, PyObject* args, PyObject* kwargs)
+{
+    int err = 0;
+    u_int32_t which, value;
+    static char* kwnames[] = { "which", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i:get_config", kwnames,
+                                     &which))
+        return NULL;
+
+    CHECK_SITE_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->site->get_config(self->site, which, &value);
+    MYDB_END_ALLOW_THREADS;
+
+    RETURN_IF_ERR();
+
+    if (value) {
+        Py_INCREF(Py_True);
+        return Py_True;
+    } else {
+        Py_INCREF(Py_False);
+        return Py_False;
+    }
+}
+
+static PyObject*
+DBSite_set_config(DBSiteObject* self, PyObject* args, PyObject* kwargs)
+{
+    int err = 0;
+    u_int32_t which, value;
+    PyObject *valueO;
+    static char* kwnames[] = { "which", "value", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "iO:set_config", kwnames,
+                                     &which, &valueO))
+        return NULL;
+
+    CHECK_SITE_NOT_CLOSED(self);
+
+    value = PyObject_IsTrue(valueO);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->site->set_config(self->site, which, value);
+    MYDB_END_ALLOW_THREADS;
+
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+#endif
+
+
+/* --------------------------------------------------------------------- */
+/* DBCursor methods */
+
+
+static PyObject*
+DBC_close_internal(DBCursorObject* self)
+{
+    int err = 0;
+
+    if (self->dbc != NULL) {
+        EXTRACT_FROM_DOUBLE_LINKED_LIST(self);
+        if (self->txn) {
+            EXTRACT_FROM_DOUBLE_LINKED_LIST_TXN(self);
+            self->txn=NULL;
+        }
+
+        MYDB_BEGIN_ALLOW_THREADS;
+        err = _DBC_close(self->dbc);
+        MYDB_END_ALLOW_THREADS;
+        self->dbc = NULL;
+    }
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBC_close(DBCursorObject* self)
+{
+    return DBC_close_internal(self);
+}
+
+
+static PyObject*
+DBC_count(DBCursorObject* self, PyObject* args)
+{
+    int err = 0;
+    db_recno_t count;
+    int flags = 0;
+
+    if (!PyArg_ParseTuple(args, "|i:count", &flags))
+        return NULL;
+
+    CHECK_CURSOR_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = _DBC_count(self->dbc, &count, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+
+    return NUMBER_FromLong(count);
+}
+
+
+static PyObject*
+DBC_current(DBCursorObject* self, PyObject* args, PyObject *kwargs)
+{
+    return _DBCursor_get(self,DB_CURRENT,args,kwargs,"|iii:current");
+}
+
+
+static PyObject*
+DBC_delete(DBCursorObject* self, PyObject* args)
+{
+    int err, flags=0;
+
+    if (!PyArg_ParseTuple(args, "|i:delete", &flags))
+        return NULL;
+
+    CHECK_CURSOR_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = _DBC_del(self->dbc, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+
+    RETURN_NONE();
+}
+
+
+static PyObject*
+DBC_dup(DBCursorObject* self, PyObject* args)
+{
+    int err, flags =0;
+    DBC* dbc = NULL;
+
+    if (!PyArg_ParseTuple(args, "|i:dup", &flags))
+        return NULL;
+
+    CHECK_CURSOR_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = _DBC_dup(self->dbc, &dbc, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+
+    return (PyObject*) newDBCursorObject(dbc, self->txn, self->mydb);
+}
+
+static PyObject*
+DBC_first(DBCursorObject* self, PyObject* args, PyObject* kwargs)
+{
+    return _DBCursor_get(self,DB_FIRST,args,kwargs,"|iii:first");
+}
+
+
+static PyObject*
+DBC_get(DBCursorObject* self, PyObject* args, PyObject *kwargs)
+{
+    int err, flags=0;
+    PyObject* keyobj = NULL;
+    PyObject* dataobj = NULL;
+    PyObject* retval = NULL;
+    int dlen = -1;
     int doff = -1;
     DBT key, data;
     static char* kwnames[] = { "key","data", "flags", "dlen", "doff",
@@ -3162,12 +4252,12 @@
     CLEAR_DBT(key);
     CLEAR_DBT(data);
     if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i|ii:get", &kwnames[2],
-				     &flags, &dlen, &doff))
+                                     &flags, &dlen, &doff))
     {
         PyErr_Clear();
         if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Oi|ii:get",
-                                         &kwnames[1], 
-					 &keyobj, &flags, &dlen, &doff))
+                                         &kwnames[1],
+                                         &keyobj, &flags, &dlen, &doff))
         {
             PyErr_Clear();
             if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OOi|ii:get",
@@ -3175,8 +4265,8 @@
                                              &flags, &dlen, &doff))
             {
                 return NULL;
-	    }
-	}
+            }
+        }
     }
 
     CHECK_CURSOR_NOT_CLOSED(self);
@@ -3186,23 +4276,16 @@
     if ( (dataobj && !make_dbt(dataobj, &data)) ||
          (!add_partial_dbt(&data, dlen, doff)) )
     {
-        FREE_DBT(key);
+        FREE_DBT(key); /* 'make_key_dbt' could do a 'malloc' */
         return NULL;
     }
 
-    if (CHECK_DBFLAG(self->mydb, DB_THREAD)) {
-        data.flags = DB_DBT_MALLOC;
-        if (!(key.flags & DB_DBT_REALLOC)) {
-            key.flags |= DB_DBT_MALLOC;
-        }
-    }
-
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->dbc->c_get(self->dbc, &key, &data, flags);
+    err = _DBC_get(self->dbc, &key, &data, flags);
     MYDB_END_ALLOW_THREADS;
 
     if ((err == DB_NOTFOUND || err == DB_KEYEMPTY)
-	    && self->mydb->moduleFlags.getReturnsNone) {
+            && self->mydb->moduleFlags.getReturnsNone) {
         Py_INCREF(Py_None);
         retval = Py_None;
     }
@@ -3217,22 +4300,18 @@
         case DB_BTREE:
         case DB_HASH:
         default:
-            retval = Py_BuildValue("s#s#", key.data, key.size,
-                                   data.data, data.size);
+            retval = BuildValue_SS(key.data, key.size, data.data, data.size);
             break;
         case DB_RECNO:
         case DB_QUEUE:
-            retval = Py_BuildValue("is#", *((db_recno_t*)key.data),
-                                   data.data, data.size);
+            retval = BuildValue_IS(*((db_recno_t*)key.data), data.data, data.size);
             break;
         }
-        FREE_DBT(data);
     }
-    FREE_DBT(key);
+    FREE_DBT(key);  /* 'make_key_dbt' could do a 'malloc' */
     return retval;
 }
 
-#if (DBVER >= 33)
 static PyObject*
 DBC_pget(DBCursorObject* self, PyObject* args, PyObject *kwargs)
 {
@@ -3249,12 +4328,12 @@
     CLEAR_DBT(key);
     CLEAR_DBT(data);
     if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i|ii:pget", &kwnames[2],
-				     &flags, &dlen, &doff))
+                                     &flags, &dlen, &doff))
     {
         PyErr_Clear();
         if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Oi|ii:pget",
-                                         kwnames_keyOnly, 
-					 &keyobj, &flags, &dlen, &doff))
+                                         kwnames_keyOnly,
+                                         &keyobj, &flags, &dlen, &doff))
         {
             PyErr_Clear();
             if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OOi|ii:pget",
@@ -3262,8 +4341,8 @@
                                              &flags, &dlen, &doff))
             {
                 return NULL;
-	    }
-	}
+            }
+        }
     }
 
     CHECK_CURSOR_NOT_CLOSED(self);
@@ -3272,26 +4351,19 @@
         return NULL;
     if ( (dataobj && !make_dbt(dataobj, &data)) ||
          (!add_partial_dbt(&data, dlen, doff)) ) {
-        FREE_DBT(key);
+        FREE_DBT(key);  /* 'make_key_dbt' could do a 'malloc' */
         return NULL;
     }
 
-    if (CHECK_DBFLAG(self->mydb, DB_THREAD)) {
-        data.flags = DB_DBT_MALLOC;
-        if (!(key.flags & DB_DBT_REALLOC)) {
-            key.flags |= DB_DBT_MALLOC;
-        }
-    }
-
     CLEAR_DBT(pkey);
     pkey.flags = DB_DBT_MALLOC;
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->dbc->c_pget(self->dbc, &key, &pkey, &data, flags);
+    err = _DBC_pget(self->dbc, &key, &pkey, &data, flags);
     MYDB_END_ALLOW_THREADS;
 
     if ((err == DB_NOTFOUND || err == DB_KEYEMPTY)
-	    && self->mydb->moduleFlags.getReturnsNone) {
+            && self->mydb->moduleFlags.getReturnsNone) {
         Py_INCREF(Py_None);
         retval = Py_None;
     }
@@ -3301,83 +4373,63 @@
     else {
         PyObject *pkeyObj;
         PyObject *dataObj;
-        dataObj = PyString_FromStringAndSize(data.data, data.size);
+        dataObj = Build_PyString(data.data, data.size);
 
         if (self->mydb->primaryDBType == DB_RECNO ||
             self->mydb->primaryDBType == DB_QUEUE)
-            pkeyObj = PyInt_FromLong(*(int *)pkey.data);
+            pkeyObj = NUMBER_FromLong(*(int *)pkey.data);
         else
-            pkeyObj = PyString_FromStringAndSize(pkey.data, pkey.size);
+            pkeyObj = Build_PyString(pkey.data, pkey.size);
 
         if (key.data && key.size) /* return key, pkey and data */
         {
             PyObject *keyObj;
             int type = _DB_get_type(self->mydb);
             if (type == DB_RECNO || type == DB_QUEUE)
-                keyObj = PyInt_FromLong(*(int *)key.data);
+                keyObj = NUMBER_FromLong(*(int *)key.data);
             else
-                keyObj = PyString_FromStringAndSize(key.data, key.size);
-#if (PY_VERSION_HEX >= 0x02040000)
+                keyObj = Build_PyString(key.data, key.size);
             retval = PyTuple_Pack(3, keyObj, pkeyObj, dataObj);
-#else
-            retval = Py_BuildValue("OOO", keyObj, pkeyObj, dataObj);
-#endif
             Py_DECREF(keyObj);
-            FREE_DBT(key);
+            FREE_DBT(key);  /* 'make_key_dbt' could do a 'malloc' */
         }
         else /* return just the pkey and data */
         {
-#if (PY_VERSION_HEX >= 0x02040000)
             retval = PyTuple_Pack(2, pkeyObj, dataObj);
-#else
-            retval = Py_BuildValue("OO", pkeyObj, dataObj);
-#endif
         }
         Py_DECREF(dataObj);
         Py_DECREF(pkeyObj);
         FREE_DBT(pkey);
-        FREE_DBT(data);
     }
     /* the only time REALLOC should be set is if we used an integer
      * key that make_key_dbt malloc'd for us.  always free these. */
-    if (key.flags & DB_DBT_REALLOC) {
+    if (key.flags & DB_DBT_REALLOC) {  /* 'make_key_dbt' could do a 'malloc' */
         FREE_DBT(key);
     }
     return retval;
 }
-#endif
 
 
 static PyObject*
-DBC_get_recno(DBCursorObject* self, PyObject* args)
+DBC_get_recno(DBCursorObject* self)
 {
     int err;
     db_recno_t recno;
     DBT key;
     DBT data;
 
-    if (!PyArg_ParseTuple(args, ":get_recno"))
-        return NULL;
-
     CHECK_CURSOR_NOT_CLOSED(self);
 
     CLEAR_DBT(key);
     CLEAR_DBT(data);
-    if (CHECK_DBFLAG(self->mydb, DB_THREAD)) {
-        /* Tell BerkeleyDB to malloc the return value (thread safe) */
-        data.flags = DB_DBT_MALLOC;
-        key.flags = DB_DBT_MALLOC;
-    }
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->dbc->c_get(self->dbc, &key, &data, DB_GET_RECNO);
+    err = _DBC_get(self->dbc, &key, &data, DB_GET_RECNO);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
 
     recno = *((db_recno_t*)data.data);
-    FREE_DBT(key);
-    FREE_DBT(data);
-    return PyInt_FromLong(recno);
+    return NUMBER_FromLong(recno);
 }
 
 
@@ -3414,7 +4466,7 @@
     int doff = -1;
 
     if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|iii:put", kwnames,
-				     &keyobj, &dataobj, &flags, &dlen, &doff))
+                                     &keyobj, &dataobj, &flags, &dlen, &doff))
         return NULL;
 
     CHECK_CURSOR_NOT_CLOSED(self);
@@ -3424,16 +4476,15 @@
     if (!make_dbt(dataobj, &data) ||
         !add_partial_dbt(&data, dlen, doff) )
     {
-        FREE_DBT(key);
+        FREE_DBT(key);  /* 'make_key_dbt' could do a 'malloc' */
         return NULL;
     }
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->dbc->c_put(self->dbc, &key, &data, flags);
+    err = _DBC_put(self->dbc, &key, &data, flags);
     MYDB_END_ALLOW_THREADS;
-    FREE_DBT(key);
+    FREE_DBT(key);  /* 'make_key_dbt' could do a 'malloc' */
     RETURN_IF_ERR();
-    self->mydb->haveStat = 0;
     RETURN_NONE();
 }
 
@@ -3449,7 +4500,7 @@
     int doff = -1;
 
     if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|iii:set", kwnames,
-				     &keyobj, &flags, &dlen, &doff))
+                                     &keyobj, &flags, &dlen, &doff))
         return NULL;
 
     CHECK_CURSOR_NOT_CLOSED(self);
@@ -3458,20 +4509,16 @@
         return NULL;
 
     CLEAR_DBT(data);
-    if (CHECK_DBFLAG(self->mydb, DB_THREAD)) {
-        /* Tell BerkeleyDB to malloc the return value (thread safe) */
-        data.flags = DB_DBT_MALLOC;
-    }
     if (!add_partial_dbt(&data, dlen, doff)) {
-        FREE_DBT(key);
+        FREE_DBT(key);  /* 'make_key_dbt' could do a 'malloc' */
         return NULL;
     }
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->dbc->c_get(self->dbc, &key, &data, flags|DB_SET);
+    err = _DBC_get(self->dbc, &key, &data, flags|DB_SET);
     MYDB_END_ALLOW_THREADS;
     if ((err == DB_NOTFOUND || err == DB_KEYEMPTY)
-	    && self->mydb->moduleFlags.cursorSetReturnsNone) {
+            && self->mydb->moduleFlags.cursorSetReturnsNone) {
         Py_INCREF(Py_None);
         retval = Py_None;
     }
@@ -3486,22 +4533,19 @@
         case DB_BTREE:
         case DB_HASH:
         default:
-            retval = Py_BuildValue("s#s#", key.data, key.size,
-                                   data.data, data.size);
+            retval = BuildValue_SS(key.data, key.size, data.data, data.size);
             break;
         case DB_RECNO:
         case DB_QUEUE:
-            retval = Py_BuildValue("is#", *((db_recno_t*)key.data),
-                                   data.data, data.size);
+            retval = BuildValue_IS(*((db_recno_t*)key.data), data.data, data.size);
             break;
         }
-        FREE_DBT(data);
-        FREE_DBT(key);
+        FREE_DBT(key);  /* 'make_key_dbt' could do a 'malloc' */
     }
     /* the only time REALLOC should be set is if we used an integer
      * key that make_key_dbt malloc'd for us.  always free these. */
     if (key.flags & DB_DBT_REALLOC) {
-        FREE_DBT(key);
+        FREE_DBT(key);  /* 'make_key_dbt' could do a 'malloc' */
     }
 
     return retval;
@@ -3519,7 +4563,7 @@
     int doff = -1;
 
     if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|iii:set_range", kwnames,
-				     &keyobj, &flags, &dlen, &doff))
+                                     &keyobj, &flags, &dlen, &doff))
         return NULL;
 
     CHECK_CURSOR_NOT_CLOSED(self);
@@ -3529,22 +4573,14 @@
 
     CLEAR_DBT(data);
     if (!add_partial_dbt(&data, dlen, doff)) {
-        FREE_DBT(key);
+        FREE_DBT(key);  /* 'make_key_dbt' could do a 'malloc' */
         return NULL;
     }
-    if (CHECK_DBFLAG(self->mydb, DB_THREAD)) {
-        /* Tell BerkeleyDB to malloc the return value (thread safe) */
-        data.flags |= DB_DBT_MALLOC;
-        /* only BTREE databases will return anything in the key */
-        if (!(key.flags & DB_DBT_REALLOC) && _DB_get_type(self->mydb) == DB_BTREE) {
-            key.flags |= DB_DBT_MALLOC;
-        }
-    }
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->dbc->c_get(self->dbc, &key, &data, flags|DB_SET_RANGE);
+    err = _DBC_get(self->dbc, &key, &data, flags|DB_SET_RANGE);
     MYDB_END_ALLOW_THREADS;
     if ((err == DB_NOTFOUND || err == DB_KEYEMPTY)
-	    && self->mydb->moduleFlags.cursorSetReturnsNone) {
+            && self->mydb->moduleFlags.cursorSetReturnsNone) {
         Py_INCREF(Py_None);
         retval = Py_None;
     }
@@ -3559,22 +4595,19 @@
         case DB_BTREE:
         case DB_HASH:
         default:
-            retval = Py_BuildValue("s#s#", key.data, key.size,
-                                   data.data, data.size);
+            retval = BuildValue_SS(key.data, key.size, data.data, data.size);
             break;
         case DB_RECNO:
         case DB_QUEUE:
-            retval = Py_BuildValue("is#", *((db_recno_t*)key.data),
-                                   data.data, data.size);
+            retval = BuildValue_IS(*((db_recno_t*)key.data), data.data, data.size);
             break;
         }
-        FREE_DBT(key);
-        FREE_DBT(data);
+        FREE_DBT(key);  /* 'make_key_dbt' could do a 'malloc' */
     }
     /* the only time REALLOC should be set is if we used an integer
      * key that make_key_dbt malloc'd for us.  always free these. */
     if (key.flags & DB_DBT_REALLOC) {
-        FREE_DBT(key);
+        FREE_DBT(key);  /* 'make_key_dbt' could do a 'malloc' */
     }
 
     return retval;
@@ -3592,12 +4625,12 @@
     if (!make_key_dbt(self->mydb, keyobj, &key, NULL))
         return NULL;
     if (!make_dbt(dataobj, &data)) {
-        FREE_DBT(key);
+        FREE_DBT(key);  /* 'make_key_dbt' could do a 'malloc' */
         return NULL;
     }
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->dbc->c_get(self->dbc, &key, &data, flags|DB_GET_BOTH);
+    err = _DBC_get(self->dbc, &key, &data, flags|DB_GET_BOTH);
     MYDB_END_ALLOW_THREADS;
     if ((err == DB_NOTFOUND || err == DB_KEYEMPTY) && returnsNone) {
         Py_INCREF(Py_None);
@@ -3614,18 +4647,16 @@
         case DB_BTREE:
         case DB_HASH:
         default:
-            retval = Py_BuildValue("s#s#", key.data, key.size,
-                                   data.data, data.size);
+            retval = BuildValue_SS(key.data, key.size, data.data, data.size);
             break;
         case DB_RECNO:
         case DB_QUEUE:
-            retval = Py_BuildValue("is#", *((db_recno_t*)key.data),
-                                   data.data, data.size);
+            retval = BuildValue_IS(*((db_recno_t*)key.data), data.data, data.size);
             break;
         }
     }
 
-    FREE_DBT(key);
+    FREE_DBT(key);  /* 'make_key_dbt' could do a 'malloc' */
     return retval;
 }
 
@@ -3647,14 +4678,12 @@
 
 /* Return size of entry */
 static PyObject*
-DBC_get_current_size(DBCursorObject* self, PyObject* args)
+DBC_get_current_size(DBCursorObject* self)
 {
     int err, flags=DB_CURRENT;
     PyObject* retval = NULL;
     DBT key, data;
 
-    if (!PyArg_ParseTuple(args, ":get_current_size"))
-        return NULL;
     CHECK_CURSOR_NOT_CLOSED(self);
     CLEAR_DBT(key);
     CLEAR_DBT(data);
@@ -3664,16 +4693,14 @@
     data.flags = DB_DBT_USERMEM;
     data.ulen = 0;
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->dbc->c_get(self->dbc, &key, &data, flags);
+    err = _DBC_get(self->dbc, &key, &data, flags);
     MYDB_END_ALLOW_THREADS;
     if (err == DB_BUFFER_SMALL || !err) {
         /* DB_BUFFER_SMALL means positive size, !err means zero length value */
-        retval = PyInt_FromLong((long)data.size);
+        retval = NUMBER_FromLong((long)data.size);
         err = 0;
     }
 
-    FREE_DBT(key);
-    FREE_DBT(data);
     RETURN_IF_ERR();
     return retval;
 }
@@ -3707,7 +4734,7 @@
     static char* kwnames[] = { "recno","flags", "dlen", "doff", NULL };
 
     if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i|iii:set_recno", kwnames,
-				     &irecno, &flags, &dlen, &doff))
+                                     &irecno, &flags, &dlen, &doff))
       return NULL;
 
     CHECK_CURSOR_NOT_CLOSED(self);
@@ -3727,20 +4754,16 @@
     key.flags = DB_DBT_REALLOC;
 
     CLEAR_DBT(data);
-    if (CHECK_DBFLAG(self->mydb, DB_THREAD)) {
-        /* Tell BerkeleyDB to malloc the return value (thread safe) */
-        data.flags = DB_DBT_MALLOC;
-    }
     if (!add_partial_dbt(&data, dlen, doff)) {
         FREE_DBT(key);
         return NULL;
     }
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->dbc->c_get(self->dbc, &key, &data, flags|DB_SET_RECNO);
+    err = _DBC_get(self->dbc, &key, &data, flags|DB_SET_RECNO);
     MYDB_END_ALLOW_THREADS;
     if ((err == DB_NOTFOUND || err == DB_KEYEMPTY)
-	    && self->mydb->moduleFlags.cursorSetReturnsNone) {
+            && self->mydb->moduleFlags.cursorSetReturnsNone) {
         Py_INCREF(Py_None);
         retval = Py_None;
     }
@@ -3748,9 +4771,7 @@
         retval = NULL;
     }
     else {  /* Can only be used for BTrees, so no need to return int key */
-        retval = Py_BuildValue("s#s#", key.data, key.size,
-                               data.data, data.size);
-        FREE_DBT(data);
+        retval = BuildValue_SS(key.data, key.size, data.data, data.size);
     }
     FREE_DBT(key);
 
@@ -3778,6 +4799,13 @@
     return _DBCursor_get(self,DB_NEXT_NODUP,args,kwargs,"|iii:next_nodup");
 }
 
+#if (DBVER >= 46)
+static PyObject*
+DBC_prev_dup(DBCursorObject* self, PyObject* args, PyObject *kwargs)
+{
+    return _DBCursor_get(self,DB_PREV_DUP,args,kwargs,"|iii:prev_dup");
+}
+#endif
 
 static PyObject*
 DBC_prev_nodup(DBCursorObject* self, PyObject* args, PyObject *kwargs)
@@ -3800,16 +4828,12 @@
 
     CLEAR_DBT(key);
     CLEAR_DBT(data);
-    if (CHECK_DBFLAG(self->mydb, DB_THREAD)) {
-        /* Tell BerkeleyDB to malloc the return value (thread safe) */
-        key.flags = DB_DBT_MALLOC;
-    }
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->dbc->c_get(self->dbc, &key, &data, flags | DB_JOIN_ITEM);
+    err = _DBC_get(self->dbc, &key, &data, flags | DB_JOIN_ITEM);
     MYDB_END_ALLOW_THREADS;
     if ((err == DB_NOTFOUND || err == DB_KEYEMPTY)
-	    && self->mydb->moduleFlags.getReturnsNone) {
+            && self->mydb->moduleFlags.getReturnsNone) {
         Py_INCREF(Py_None);
         retval = Py_None;
     }
@@ -3817,39 +4841,106 @@
         retval = NULL;
     }
     else {
-        retval = Py_BuildValue("s#", key.data, key.size);
-        FREE_DBT(key);
+        retval = BuildValue_S(key.data, key.size);
     }
 
     return retval;
 }
 
 
+#if (DBVER >= 46)
+static PyObject*
+DBC_set_priority(DBCursorObject* self, PyObject* args, PyObject* kwargs)
+{
+    int err, priority;
+    static char* kwnames[] = { "priority", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i:set_priority", kwnames,
+                                     &priority))
+        return NULL;
+
+    CHECK_CURSOR_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->dbc->set_priority(self->dbc, priority);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+
+static PyObject*
+DBC_get_priority(DBCursorObject* self)
+{
+    int err;
+    DB_CACHE_PRIORITY priority;
+
+    CHECK_CURSOR_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->dbc->get_priority(self->dbc, &priority);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(priority);
+}
+#endif
+
+
 
 /* --------------------------------------------------------------------- */
 /* DBEnv methods */
 
 
 static PyObject*
-DBEnv_close(DBEnvObject* self, PyObject* args)
+DBEnv_close_internal(DBEnvObject* self, int flags)
 {
-    int err, flags = 0;
+    PyObject *dummy;
+    int err;
 
-    if (!PyArg_ParseTuple(args, "|i:close", &flags))
-        return NULL;
     if (!self->closed) {      /* Don't close more than once */
+        while(self->children_txns) {
+            dummy = DBTxn_abort_discard_internal(self->children_txns, 0);
+            Py_XDECREF(dummy);
+        }
+        while(self->children_dbs) {
+            dummy = DB_close_internal(self->children_dbs, 0, 0);
+            Py_XDECREF(dummy);
+        }
+        while(self->children_logcursors) {
+            dummy = DBLogCursor_close_internal(self->children_logcursors);
+            Py_XDECREF(dummy);
+        }
+#if (DBVER >= 52)
+        while(self->children_sites) {
+            dummy = DBSite_close_internal(self->children_sites);
+            Py_XDECREF(dummy);
+        }
+#endif
+    }
+
+    self->closed = 1;
+    if (self->db_env) {
         MYDB_BEGIN_ALLOW_THREADS;
         err = self->db_env->close(self->db_env, flags);
         MYDB_END_ALLOW_THREADS;
         /* after calling DBEnv->close, regardless of error, this DBEnv
-         * may not be accessed again (BerkeleyDB docs). */
-        self->closed = 1;
+         * may not be accessed again (Berkeley DB docs). */
         self->db_env = NULL;
         RETURN_IF_ERR();
     }
     RETURN_NONE();
 }
 
+static PyObject*
+DBEnv_close(DBEnvObject* self, PyObject* args)
+{
+    int flags = 0;
+
+    if (!PyArg_ParseTuple(args, "|i:close", &flags))
+        return NULL;
+    return DBEnv_close_internal(self, flags);
+}
+
 
 static PyObject*
 DBEnv_open(DBEnvObject* self, PyObject* args)
@@ -3873,10 +4964,196 @@
 
 
 static PyObject*
-DBEnv_remove(DBEnvObject* self, PyObject* args)
+DBEnv_memp_stat(DBEnvObject* self, PyObject* args, PyObject *kwargs)
 {
-    int err, flags=0;
-    char *db_home;
+    int err;
+    DB_MPOOL_STAT *gsp;
+    DB_MPOOL_FSTAT **fsp, **fsp2;
+    PyObject* d = NULL, *d2, *d3, *r;
+    u_int32_t flags = 0;
+    static char* kwnames[] = { "flags", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i:memp_stat",
+                kwnames, &flags))
+        return NULL;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->memp_stat(self->db_env, &gsp, &fsp, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+
+    /* Turn the stat structure into a dictionary */
+    d = PyDict_New();
+    if (d == NULL) {
+        if (gsp)
+            free(gsp);
+        return NULL;
+    }
+
+#define MAKE_ENTRY(name)  _addIntToDict(d, #name, gsp->st_##name)
+
+    MAKE_ENTRY(gbytes);
+    MAKE_ENTRY(bytes);
+    MAKE_ENTRY(ncache);
+#if (DBVER >= 46)
+    MAKE_ENTRY(max_ncache);
+#endif
+    MAKE_ENTRY(regsize);
+    MAKE_ENTRY(mmapsize);
+    MAKE_ENTRY(maxopenfd);
+    MAKE_ENTRY(maxwrite);
+    MAKE_ENTRY(maxwrite_sleep);
+    MAKE_ENTRY(map);
+    MAKE_ENTRY(cache_hit);
+    MAKE_ENTRY(cache_miss);
+    MAKE_ENTRY(page_create);
+    MAKE_ENTRY(page_in);
+    MAKE_ENTRY(page_out);
+    MAKE_ENTRY(ro_evict);
+    MAKE_ENTRY(rw_evict);
+    MAKE_ENTRY(page_trickle);
+    MAKE_ENTRY(pages);
+    MAKE_ENTRY(page_clean);
+    MAKE_ENTRY(page_dirty);
+    MAKE_ENTRY(hash_buckets);
+    MAKE_ENTRY(hash_searches);
+    MAKE_ENTRY(hash_longest);
+    MAKE_ENTRY(hash_examined);
+    MAKE_ENTRY(hash_nowait);
+    MAKE_ENTRY(hash_wait);
+#if (DBVER >= 45)
+    MAKE_ENTRY(hash_max_nowait);
+#endif
+    MAKE_ENTRY(hash_max_wait);
+    MAKE_ENTRY(region_wait);
+    MAKE_ENTRY(region_nowait);
+#if (DBVER >= 45)
+    MAKE_ENTRY(mvcc_frozen);
+    MAKE_ENTRY(mvcc_thawed);
+    MAKE_ENTRY(mvcc_freed);
+#endif
+    MAKE_ENTRY(alloc);
+    MAKE_ENTRY(alloc_buckets);
+    MAKE_ENTRY(alloc_max_buckets);
+    MAKE_ENTRY(alloc_pages);
+    MAKE_ENTRY(alloc_max_pages);
+#if (DBVER >= 45)
+    MAKE_ENTRY(io_wait);
+#endif
+#if (DBVER >= 48)
+    MAKE_ENTRY(sync_interrupted);
+#endif
+
+#undef MAKE_ENTRY
+    free(gsp);
+
+    d2 = PyDict_New();
+    if (d2 == NULL) {
+        Py_DECREF(d);
+        if (fsp)
+            free(fsp);
+        return NULL;
+    }
+#define MAKE_ENTRY(name)  _addIntToDict(d3, #name, (*fsp2)->st_##name)
+    for(fsp2=fsp;*fsp2; fsp2++) {
+        d3 = PyDict_New();
+        if (d3 == NULL) {
+            Py_DECREF(d);
+            Py_DECREF(d2);
+            if (fsp)
+                free(fsp);
+            return NULL;
+        }
+        MAKE_ENTRY(pagesize);
+        MAKE_ENTRY(cache_hit);
+        MAKE_ENTRY(cache_miss);
+        MAKE_ENTRY(map);
+        MAKE_ENTRY(page_create);
+        MAKE_ENTRY(page_in);
+        MAKE_ENTRY(page_out);
+        if(PyDict_SetItemString(d2, (*fsp2)->file_name, d3)) {
+            Py_DECREF(d);
+            Py_DECREF(d2);
+            Py_DECREF(d3);
+            if (fsp)
+                free(fsp);
+            return NULL;
+        }
+        Py_DECREF(d3);
+    }
+
+#undef MAKE_ENTRY
+    free(fsp);
+
+    r = PyTuple_Pack(2, d, d2);
+    Py_DECREF(d);
+    Py_DECREF(d2);
+    return r;
+}
+
+static PyObject*
+DBEnv_memp_stat_print(DBEnvObject* self, PyObject* args, PyObject *kwargs)
+{
+    int err;
+    int flags=0;
+    static char* kwnames[] = { "flags", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i:memp_stat_print",
+                kwnames, &flags))
+    {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->memp_stat_print(self->db_env, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+
+static PyObject*
+DBEnv_memp_trickle(DBEnvObject* self, PyObject* args)
+{
+    int err, percent, nwrotep;
+
+    if (!PyArg_ParseTuple(args, "i:memp_trickle", &percent))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->memp_trickle(self->db_env, percent, &nwrotep);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(nwrotep);
+}
+
+static PyObject*
+DBEnv_memp_sync(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    DB_LSN lsn = {0, 0};
+    DB_LSN *lsn_p = NULL;
+
+    if (!PyArg_ParseTuple(args, "|(ii):memp_sync", &lsn.file, &lsn.offset))
+        return NULL;
+    if ((lsn.file!=0) || (lsn.offset!=0)) {
+        lsn_p = &lsn;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->memp_sync(self->db_env, lsn_p);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_remove(DBEnvObject* self, PyObject* args)
+{
+    int err, flags=0;
+    char *db_home;
 
     if (!PyArg_ParseTuple(args, "s|i:remove", &db_home, &flags))
         return NULL;
@@ -3888,7 +5165,6 @@
     RETURN_NONE();
 }
 
-#if (DBVER >= 41)
 static PyObject*
 DBEnv_dbremove(DBEnvObject* self, PyObject* args, PyObject* kwargs)
 {
@@ -3902,8 +5178,8 @@
                                      NULL };
 
     if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|zOi:dbremove", kwnames,
-		&file, &database, &txnobj, &flags)) {
-	return NULL;
+                &file, &database, &txnobj, &flags)) {
+        return NULL;
     }
     if (!checkTxnObj(txnobj, &txn)) {
         return NULL;
@@ -3930,8 +5206,8 @@
                                      "flags", NULL };
 
     if (!PyArg_ParseTupleAndKeywords(args, kwargs, "szs|Oi:dbrename", kwnames,
-		&file, &database, &newname, &txnobj, &flags)) {
-	return NULL;
+                &file, &database, &newname, &txnobj, &flags)) {
+        return NULL;
     }
     if (!checkTxnObj(txnobj, &txn)) {
         return NULL;
@@ -3945,6 +5221,8 @@
     RETURN_NONE();
 }
 
+
+
 static PyObject*
 DBEnv_set_encrypt(DBEnvObject* self, PyObject* args, PyObject* kwargs)
 {
@@ -3954,8 +5232,8 @@
     static char* kwnames[] = { "passwd", "flags", NULL };
 
     if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|i:set_encrypt", kwnames,
-		&passwd, &flags)) {
-	return NULL;
+                &passwd, &flags)) {
+        return NULL;
     }
 
     MYDB_BEGIN_ALLOW_THREADS;
@@ -3965,9 +5243,46 @@
     RETURN_IF_ERR();
     RETURN_NONE();
 }
-#endif /* DBVER >= 41 */
 
-#if (DBVER >= 40)
+static PyObject*
+DBEnv_get_encrypt_flags(DBEnvObject* self)
+{
+    int err;
+    u_int32_t flags;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_encrypt_flags(self->db_env, &flags);
+    MYDB_END_ALLOW_THREADS;
+
+    RETURN_IF_ERR();
+
+    return NUMBER_FromLong(flags);
+}
+
+static PyObject*
+DBEnv_get_timeout(DBEnvObject* self, PyObject* args, PyObject* kwargs)
+{
+    int err;
+    int flag;
+    u_int32_t timeout;
+    static char* kwnames[] = {"flag", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i:get_timeout", kwnames,
+                &flag)) {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_timeout(self->db_env, &timeout, flag);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(timeout);
+}
+
+
 static PyObject*
 DBEnv_set_timeout(DBEnvObject* self, PyObject* args, PyObject* kwargs)
 {
@@ -3977,8 +5292,8 @@
     static char* kwnames[] = { "timeout", "flags", NULL };
 
     if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ii:set_timeout", kwnames,
-		&timeout, &flags)) {
-	return NULL;
+                &timeout, &flags)) {
+        return NULL;
     }
 
     MYDB_BEGIN_ALLOW_THREADS;
@@ -3988,7 +5303,6 @@
     RETURN_IF_ERR();
     RETURN_NONE();
 }
-#endif /* DBVER >= 40 */
 
 static PyObject*
 DBEnv_set_shm_key(DBEnvObject* self, PyObject* args)
@@ -4006,695 +5320,2807 @@
 }
 
 static PyObject*
-DBEnv_set_cachesize(DBEnvObject* self, PyObject* args)
+DBEnv_get_shm_key(DBEnvObject* self)
 {
-    int err, gbytes=0, bytes=0, ncache=0;
+    int err;
+    long shm_key;
 
-    if (!PyArg_ParseTuple(args, "ii|i:set_cachesize",
-                          &gbytes, &bytes, &ncache))
-        return NULL;
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db_env->set_cachesize(self->db_env, gbytes, bytes, ncache);
+    err = self->db_env->get_shm_key(self->db_env, &shm_key);
     MYDB_END_ALLOW_THREADS;
+
     RETURN_IF_ERR();
-    RETURN_NONE();
-}
 
+    return NUMBER_FromLong(shm_key);
+}
 
-#if (DBVER >= 32)
+#if (DBVER >= 46)
 static PyObject*
-DBEnv_set_flags(DBEnvObject* self, PyObject* args)
+DBEnv_set_cache_max(DBEnvObject* self, PyObject* args)
 {
-    int err, flags=0, onoff=0;
+    int err, gbytes, bytes;
 
-    if (!PyArg_ParseTuple(args, "ii:set_flags",
-                          &flags, &onoff))
+    if (!PyArg_ParseTuple(args, "ii:set_cache_max",
+                          &gbytes, &bytes))
         return NULL;
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db_env->set_flags(self->db_env, flags, onoff);
+    err = self->db_env->set_cache_max(self->db_env, gbytes, bytes);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
     RETURN_NONE();
 }
-#endif
-
 
 static PyObject*
-DBEnv_set_data_dir(DBEnvObject* self, PyObject* args)
+DBEnv_get_cache_max(DBEnvObject* self)
 {
     int err;
-    char *dir;
+    u_int32_t gbytes, bytes;
 
-    if (!PyArg_ParseTuple(args, "s:set_data_dir", &dir))
-        return NULL;
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db_env->set_data_dir(self->db_env, dir);
+    err = self->db_env->get_cache_max(self->db_env, &gbytes, &bytes);
     MYDB_END_ALLOW_THREADS;
+
     RETURN_IF_ERR();
-    RETURN_NONE();
-}
 
+    return Py_BuildValue("(ii)", gbytes, bytes);
+}
+#endif
 
+#if (DBVER >= 46)
 static PyObject*
-DBEnv_set_lg_bsize(DBEnvObject* self, PyObject* args)
+DBEnv_set_thread_count(DBEnvObject* self, PyObject* args)
 {
-    int err, lg_bsize;
+    int err;
+    u_int32_t count;
 
-    if (!PyArg_ParseTuple(args, "i:set_lg_bsize", &lg_bsize))
+    if (!PyArg_ParseTuple(args, "i:set_thread_count", &count))
         return NULL;
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db_env->set_lg_bsize(self->db_env, lg_bsize);
+    err = self->db_env->set_thread_count(self->db_env, count);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
     RETURN_NONE();
 }
 
-
 static PyObject*
-DBEnv_set_lg_dir(DBEnvObject* self, PyObject* args)
+DBEnv_get_thread_count(DBEnvObject* self)
 {
     int err;
-    char *dir;
+    u_int32_t count;
 
-    if (!PyArg_ParseTuple(args, "s:set_lg_dir", &dir))
-        return NULL;
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db_env->set_lg_dir(self->db_env, dir);
+    err = self->db_env->get_thread_count(self->db_env, &count);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
-    RETURN_NONE();
+    return NUMBER_FromLong(count);
 }
+#endif
 
 static PyObject*
-DBEnv_set_lg_max(DBEnvObject* self, PyObject* args)
+DBEnv_set_cachesize(DBEnvObject* self, PyObject* args)
 {
-    int err, lg_max;
+    int err, gbytes=0, bytes=0, ncache=0;
 
-    if (!PyArg_ParseTuple(args, "i:set_lg_max", &lg_max))
+    if (!PyArg_ParseTuple(args, "ii|i:set_cachesize",
+                          &gbytes, &bytes, &ncache))
         return NULL;
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db_env->set_lg_max(self->db_env, lg_max);
+    err = self->db_env->set_cachesize(self->db_env, gbytes, bytes, ncache);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
     RETURN_NONE();
 }
 
-
-#if (DBVER >= 33)
 static PyObject*
-DBEnv_set_lg_regionmax(DBEnvObject* self, PyObject* args)
+DBEnv_get_cachesize(DBEnvObject* self)
 {
-    int err, lg_max;
+    int err;
+    u_int32_t gbytes, bytes;
+    int ncache;
 
-    if (!PyArg_ParseTuple(args, "i:set_lg_regionmax", &lg_max))
-        return NULL;
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db_env->set_lg_regionmax(self->db_env, lg_max);
+    err = self->db_env->get_cachesize(self->db_env, &gbytes, &bytes, &ncache);
     MYDB_END_ALLOW_THREADS;
+
     RETURN_IF_ERR();
-    RETURN_NONE();
+
+    return Py_BuildValue("(iii)", gbytes, bytes, ncache);
 }
-#endif
 
 
 static PyObject*
-DBEnv_set_lk_detect(DBEnvObject* self, PyObject* args)
+DBEnv_set_flags(DBEnvObject* self, PyObject* args)
 {
-    int err, lk_detect;
+    int err, flags=0, onoff=0;
 
-    if (!PyArg_ParseTuple(args, "i:set_lk_detect", &lk_detect))
+    if (!PyArg_ParseTuple(args, "ii:set_flags",
+                          &flags, &onoff))
         return NULL;
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db_env->set_lk_detect(self->db_env, lk_detect);
+    err = self->db_env->set_flags(self->db_env, flags, onoff);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
     RETURN_NONE();
 }
 
-
-#if (DBVER < 45)
 static PyObject*
-DBEnv_set_lk_max(DBEnvObject* self, PyObject* args)
+DBEnv_get_flags(DBEnvObject* self)
 {
-    int err, max;
+    int err;
+    u_int32_t flags;
 
-    if (!PyArg_ParseTuple(args, "i:set_lk_max", &max))
-        return NULL;
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db_env->set_lk_max(self->db_env, max);
+    err = self->db_env->get_flags(self->db_env, &flags);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
-    RETURN_NONE();
+    return NUMBER_FromLong(flags);
 }
-#endif
-
-
-#if (DBVER >= 32)
 
+#if (DBVER >= 47)
 static PyObject*
-DBEnv_set_lk_max_locks(DBEnvObject* self, PyObject* args)
+DBEnv_log_set_config(DBEnvObject* self, PyObject* args)
 {
-    int err, max;
+    int err, flags, onoff;
 
-    if (!PyArg_ParseTuple(args, "i:set_lk_max_locks", &max))
+    if (!PyArg_ParseTuple(args, "ii:log_set_config",
+                          &flags, &onoff))
         return NULL;
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db_env->set_lk_max_locks(self->db_env, max);
+    err = self->db_env->log_set_config(self->db_env, flags, onoff);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
     RETURN_NONE();
 }
 
-
 static PyObject*
-DBEnv_set_lk_max_lockers(DBEnvObject* self, PyObject* args)
+DBEnv_log_get_config(DBEnvObject* self, PyObject* args)
 {
-    int err, max;
+    int err, flag, onoff;
 
-    if (!PyArg_ParseTuple(args, "i:set_lk_max_lockers", &max))
+    if (!PyArg_ParseTuple(args, "i:log_get_config", &flag))
         return NULL;
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db_env->set_lk_max_lockers(self->db_env, max);
+    err = self->db_env->log_get_config(self->db_env, flag, &onoff);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
-    RETURN_NONE();
+    return PyBool_FromLong(onoff);
 }
+#endif /* DBVER >= 47 */
 
-
+#if (DBVER >= 44)
 static PyObject*
-DBEnv_set_lk_max_objects(DBEnvObject* self, PyObject* args)
+DBEnv_mutex_set_max(DBEnvObject* self, PyObject* args)
 {
-    int err, max;
+    int err;
+    int value;
 
-    if (!PyArg_ParseTuple(args, "i:set_lk_max_objects", &max))
+    if (!PyArg_ParseTuple(args, "i:mutex_set_max", &value))
         return NULL;
+
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db_env->set_lk_max_objects(self->db_env, max);
+    err = self->db_env->mutex_set_max(self->db_env, value);
     MYDB_END_ALLOW_THREADS;
+
     RETURN_IF_ERR();
     RETURN_NONE();
 }
 
-#endif
-
-
 static PyObject*
-DBEnv_set_mp_mmapsize(DBEnvObject* self, PyObject* args)
+DBEnv_mutex_get_max(DBEnvObject* self)
 {
-    int err, mp_mmapsize;
+    int err;
+    u_int32_t value;
 
-    if (!PyArg_ParseTuple(args, "i:set_mp_mmapsize", &mp_mmapsize))
-        return NULL;
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db_env->set_mp_mmapsize(self->db_env, mp_mmapsize);
+    err = self->db_env->mutex_get_max(self->db_env, &value);
     MYDB_END_ALLOW_THREADS;
+
     RETURN_IF_ERR();
-    RETURN_NONE();
-}
 
+    return NUMBER_FromLong(value);
+}
 
 static PyObject*
-DBEnv_set_tmp_dir(DBEnvObject* self, PyObject* args)
+DBEnv_mutex_set_align(DBEnvObject* self, PyObject* args)
 {
     int err;
-    char *dir;
+    int align;
 
-    if (!PyArg_ParseTuple(args, "s:set_tmp_dir", &dir))
+    if (!PyArg_ParseTuple(args, "i:mutex_set_align", &align))
         return NULL;
+
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db_env->set_tmp_dir(self->db_env, dir);
+    err = self->db_env->mutex_set_align(self->db_env, align);
     MYDB_END_ALLOW_THREADS;
+
     RETURN_IF_ERR();
     RETURN_NONE();
 }
 
-
 static PyObject*
-DBEnv_txn_begin(DBEnvObject* self, PyObject* args, PyObject* kwargs)
+DBEnv_mutex_get_align(DBEnvObject* self)
 {
-    int flags = 0;
-    PyObject* txnobj = NULL;
-    DB_TXN *txn = NULL;
-    static char* kwnames[] = { "parent", "flags", NULL };
+    int err;
+    u_int32_t align;
 
-    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|Oi:txn_begin", kwnames,
-                                     &txnobj, &flags))
-        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
 
-    if (!checkTxnObj(txnobj, &txn))
-        return NULL;
-    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->mutex_get_align(self->db_env, &align);
+    MYDB_END_ALLOW_THREADS;
 
-    return (PyObject*)newDBTxnObject(self, txn, flags);
-}
+    RETURN_IF_ERR();
 
+    return NUMBER_FromLong(align);
+}
 
 static PyObject*
-DBEnv_txn_checkpoint(DBEnvObject* self, PyObject* args)
+DBEnv_mutex_set_increment(DBEnvObject* self, PyObject* args)
 {
-    int err, kbyte=0, min=0, flags=0;
+    int err;
+    int increment;
 
-    if (!PyArg_ParseTuple(args, "|iii:txn_checkpoint", &kbyte, &min, &flags))
+    if (!PyArg_ParseTuple(args, "i:mutex_set_increment", &increment))
         return NULL;
+
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-#if (DBVER >= 40)
-    err = self->db_env->txn_checkpoint(self->db_env, kbyte, min, flags);
-#else
-    err = txn_checkpoint(self->db_env, kbyte, min, flags);
-#endif
+    err = self->db_env->mutex_set_increment(self->db_env, increment);
     MYDB_END_ALLOW_THREADS;
+
     RETURN_IF_ERR();
     RETURN_NONE();
 }
 
+static PyObject*
+DBEnv_mutex_get_increment(DBEnvObject* self)
+{
+    int err;
+    u_int32_t increment;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->mutex_get_increment(self->db_env, &increment);
+    MYDB_END_ALLOW_THREADS;
+
+    RETURN_IF_ERR();
+
+    return NUMBER_FromLong(increment);
+}
 
 static PyObject*
-DBEnv_set_tx_max(DBEnvObject* self, PyObject* args)
+DBEnv_mutex_set_tas_spins(DBEnvObject* self, PyObject* args)
 {
-    int err, max;
+    int err;
+    int tas_spins;
 
-    if (!PyArg_ParseTuple(args, "i:set_tx_max", &max))
+    if (!PyArg_ParseTuple(args, "i:mutex_set_tas_spins", &tas_spins))
         return NULL;
+
     CHECK_ENV_NOT_CLOSED(self);
 
-    err = self->db_env->set_tx_max(self->db_env, max);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->mutex_set_tas_spins(self->db_env, tas_spins);
+    MYDB_END_ALLOW_THREADS;
+
     RETURN_IF_ERR();
     RETURN_NONE();
 }
 
+static PyObject*
+DBEnv_mutex_get_tas_spins(DBEnvObject* self)
+{
+    int err;
+    u_int32_t tas_spins;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->mutex_get_tas_spins(self->db_env, &tas_spins);
+    MYDB_END_ALLOW_THREADS;
+
+    RETURN_IF_ERR();
+
+    return NUMBER_FromLong(tas_spins);
+}
+#endif
 
 static PyObject*
-DBEnv_set_tx_timestamp(DBEnvObject* self, PyObject* args)
+DBEnv_set_data_dir(DBEnvObject* self, PyObject* args)
 {
     int err;
-    long stamp;
-    time_t timestamp;
+    char *dir;
 
-    if (!PyArg_ParseTuple(args, "l:set_tx_timestamp", &stamp))
+    if (!PyArg_ParseTuple(args, "s:set_data_dir", &dir))
         return NULL;
     CHECK_ENV_NOT_CLOSED(self);
-    timestamp = (time_t)stamp;
-    err = self->db_env->set_tx_timestamp(self->db_env, &timestamp);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_data_dir(self->db_env, dir);
+    MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
     RETURN_NONE();
 }
 
+static PyObject*
+DBEnv_get_data_dirs(DBEnvObject* self)
+{
+    int err;
+    PyObject *tuple;
+    PyObject *item;
+    const char **dirpp;
+    int size, i;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_data_dirs(self->db_env, &dirpp);
+    MYDB_END_ALLOW_THREADS;
+
+    RETURN_IF_ERR();
+
+    /*
+    ** Calculate size. Python C API
+    ** actually allows for tuple resizing,
+    ** but this is simple enough.
+    */
+    for (size=0; *(dirpp+size) ; size++);
+
+    tuple = PyTuple_New(size);
+    if (!tuple)
+        return NULL;
+
+    for (i=0; i<size; i++) {
+        item = PyBytes_FromString (*(dirpp+i));
+        if (item == NULL) {
+            Py_DECREF(tuple);
+            tuple = NULL;
+            break;
+        }
+        PyTuple_SET_ITEM(tuple, i, item);
+    }
+    return tuple;
+}
 
+#if (DBVER >= 44)
 static PyObject*
-DBEnv_lock_detect(DBEnvObject* self, PyObject* args)
+DBEnv_set_lg_filemode(DBEnvObject* self, PyObject* args)
 {
-    int err, atype, flags=0;
-    int aborted = 0;
+    int err, filemode;
 
-    if (!PyArg_ParseTuple(args, "i|i:lock_detect", &atype, &flags))
+    if (!PyArg_ParseTuple(args, "i:set_lg_filemode", &filemode))
         return NULL;
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-#if (DBVER >= 40)
-    err = self->db_env->lock_detect(self->db_env, flags, atype, &aborted);
-#else
-    err = lock_detect(self->db_env, flags, atype, &aborted);
-#endif
+    err = self->db_env->set_lg_filemode(self->db_env, filemode);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
-    return PyInt_FromLong(aborted);
+    RETURN_NONE();
 }
 
-
 static PyObject*
-DBEnv_lock_get(DBEnvObject* self, PyObject* args)
+DBEnv_get_lg_filemode(DBEnvObject* self)
 {
-    int flags=0;
-    int locker, lock_mode;
-    DBT obj;
-    PyObject* objobj;
+    int err, filemode;
 
-    if (!PyArg_ParseTuple(args, "iOi|i:lock_get", &locker, &objobj, &lock_mode, &flags))
-        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_lg_filemode(self->db_env, &filemode);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(filemode);
+}
+#endif
 
+static PyObject*
+DBEnv_set_lg_bsize(DBEnvObject* self, PyObject* args)
+{
+    int err, lg_bsize;
 
-    if (!make_dbt(objobj, &obj))
+    if (!PyArg_ParseTuple(args, "i:set_lg_bsize", &lg_bsize))
         return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
 
-    return (PyObject*)newDBLockObject(self, locker, &obj, lock_mode, flags);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_lg_bsize(self->db_env, lg_bsize);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
 }
 
-
 static PyObject*
-DBEnv_lock_id(DBEnvObject* self, PyObject* args)
+DBEnv_get_lg_bsize(DBEnvObject* self)
 {
     int err;
-    u_int32_t theID;
-
-    if (!PyArg_ParseTuple(args, ":lock_id"))
-        return NULL;
+    u_int32_t lg_bsize;
 
     CHECK_ENV_NOT_CLOSED(self);
+
     MYDB_BEGIN_ALLOW_THREADS;
-#if (DBVER >= 40)
-    err = self->db_env->lock_id(self->db_env, &theID);
-#else
-    err = lock_id(self->db_env, &theID);
-#endif
+    err = self->db_env->get_lg_bsize(self->db_env, &lg_bsize);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
-
-    return PyInt_FromLong((long)theID);
+    return NUMBER_FromLong(lg_bsize);
 }
 
-
 static PyObject*
-DBEnv_lock_put(DBEnvObject* self, PyObject* args)
+DBEnv_set_lg_dir(DBEnvObject* self, PyObject* args)
 {
     int err;
-    DBLockObject* dblockobj;
+    char *dir;
 
-    if (!PyArg_ParseTuple(args, "O!:lock_put", &DBLock_Type, &dblockobj))
+    if (!PyArg_ParseTuple(args, "s:set_lg_dir", &dir))
         return NULL;
-
     CHECK_ENV_NOT_CLOSED(self);
+
     MYDB_BEGIN_ALLOW_THREADS;
-#if (DBVER >= 40)
-    err = self->db_env->lock_put(self->db_env, &dblockobj->lock);
-#else
-    err = lock_put(self->db_env, &dblockobj->lock);
-#endif
+    err = self->db_env->set_lg_dir(self->db_env, dir);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
     RETURN_NONE();
 }
 
-#if (DBVER >= 44)
 static PyObject*
-DBEnv_lsn_reset(DBEnvObject* self, PyObject* args, PyObject* kwargs)
+DBEnv_get_lg_dir(DBEnvObject* self)
 {
     int err;
-    char *file;
-    u_int32_t flags = 0;
-    static char* kwnames[] = { "file", "flags", NULL};
+    const char *dirp;
 
-    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "z|i:lsn_reset", kwnames,
-                                     &file, &flags))
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_lg_dir(self->db_env, &dirp);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return PyBytes_FromString(dirp);
+}
+
+static PyObject*
+DBEnv_set_lg_max(DBEnvObject* self, PyObject* args)
+{
+    int err, lg_max;
+
+    if (!PyArg_ParseTuple(args, "i:set_lg_max", &lg_max))
         return NULL;
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db_env->lsn_reset(self->db_env, file, flags);
+    err = self->db_env->set_lg_max(self->db_env, lg_max);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
     RETURN_NONE();
 }
-#endif /* DBVER >= 4.4 */
 
-#if (DBVER >= 40)
 static PyObject*
-DBEnv_log_stat(DBEnvObject* self, PyObject* args)
+DBEnv_get_lg_max(DBEnvObject* self)
 {
     int err;
-    DB_LOG_STAT* statp = NULL;
-    PyObject* d = NULL;
-    u_int32_t flags = 0;
+    u_int32_t lg_max;
 
-    if (!PyArg_ParseTuple(args, "|i:log_stat", &flags))
-        return NULL;
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-    err = self->db_env->log_stat(self->db_env, &statp, flags);
+    err = self->db_env->get_lg_max(self->db_env, &lg_max);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
+    return NUMBER_FromLong(lg_max);
+}
 
-    /* Turn the stat structure into a dictionary */
-    d = PyDict_New();
-    if (d == NULL) {
-        if (statp)
-            free(statp);
+static PyObject*
+DBEnv_set_lg_regionmax(DBEnvObject* self, PyObject* args)
+{
+    int err, lg_max;
+
+    if (!PyArg_ParseTuple(args, "i:set_lg_regionmax", &lg_max))
         return NULL;
-    }
+    CHECK_ENV_NOT_CLOSED(self);
 
-#define MAKE_ENTRY(name)  _addIntToDict(d, #name, statp->st_##name)
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_lg_regionmax(self->db_env, lg_max);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
 
-    MAKE_ENTRY(magic);
-    MAKE_ENTRY(version);
-    MAKE_ENTRY(mode);
-    MAKE_ENTRY(lg_bsize);
-#if (DBVER >= 44)
-    MAKE_ENTRY(lg_size);
-    MAKE_ENTRY(record);
-#endif
-#if (DBVER <= 40)
-    MAKE_ENTRY(lg_max);
-#endif
-    MAKE_ENTRY(w_mbytes);
-    MAKE_ENTRY(w_bytes);
-    MAKE_ENTRY(wc_mbytes);
-    MAKE_ENTRY(wc_bytes);
-    MAKE_ENTRY(wcount);
-    MAKE_ENTRY(wcount_fill);
-#if (DBVER >= 44)
-    MAKE_ENTRY(rcount);
-#endif
-    MAKE_ENTRY(scount);
-    MAKE_ENTRY(cur_file);
-    MAKE_ENTRY(cur_offset);
-    MAKE_ENTRY(disk_file);
-    MAKE_ENTRY(disk_offset);
-    MAKE_ENTRY(maxcommitperflush);
-    MAKE_ENTRY(mincommitperflush);
-    MAKE_ENTRY(regsize);
-    MAKE_ENTRY(region_wait);
-    MAKE_ENTRY(region_nowait);
+static PyObject*
+DBEnv_get_lg_regionmax(DBEnvObject* self)
+{
+    int err;
+    u_int32_t lg_regionmax;
 
-#undef MAKE_ENTRY
-    free(statp);
-    return d;
-} /* DBEnv_log_stat */
-#endif /* DBVER >= 4.0 for log_stat method */
+    CHECK_ENV_NOT_CLOSED(self);
 
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_lg_regionmax(self->db_env, &lg_regionmax);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(lg_regionmax);
+}
 
+#if (DBVER >= 47)
 static PyObject*
-DBEnv_lock_stat(DBEnvObject* self, PyObject* args)
+DBEnv_set_lk_partitions(DBEnvObject* self, PyObject* args)
 {
-    int err;
-    DB_LOCK_STAT* sp;
-    PyObject* d = NULL;
-    u_int32_t flags = 0;
+    int err, lk_partitions;
 
-    if (!PyArg_ParseTuple(args, "|i:lock_stat", &flags))
+    if (!PyArg_ParseTuple(args, "i:set_lk_partitions", &lk_partitions))
         return NULL;
     CHECK_ENV_NOT_CLOSED(self);
 
     MYDB_BEGIN_ALLOW_THREADS;
-#if (DBVER >= 40)
-    err = self->db_env->lock_stat(self->db_env, &sp, flags);
-#else
-#if (DBVER >= 33)
-    err = lock_stat(self->db_env, &sp);
-#else
-    err = lock_stat(self->db_env, &sp, NULL);
-#endif
-#endif
+    err = self->db_env->set_lk_partitions(self->db_env, lk_partitions);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
+    RETURN_NONE();
+}
 
-    /* Turn the stat structure into a dictionary */
-    d = PyDict_New();
-    if (d == NULL) {
-        free(sp);
-        return NULL;
-    }
+static PyObject*
+DBEnv_get_lk_partitions(DBEnvObject* self)
+{
+    int err;
+    u_int32_t lk_partitions;
 
-#define MAKE_ENTRY(name)  _addIntToDict(d, #name, sp->st_##name)
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_lk_partitions(self->db_env, &lk_partitions);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(lk_partitions);
+}
+#endif
+
+static PyObject*
+DBEnv_set_lk_detect(DBEnvObject* self, PyObject* args)
+{
+    int err, lk_detect;
+
+    if (!PyArg_ParseTuple(args, "i:set_lk_detect", &lk_detect))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_lk_detect(self->db_env, lk_detect);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_get_lk_detect(DBEnvObject* self)
+{
+    int err;
+    u_int32_t lk_detect;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_lk_detect(self->db_env, &lk_detect);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(lk_detect);
+}
+
+#if (DBVER < 45)
+static PyObject*
+DBEnv_set_lk_max(DBEnvObject* self, PyObject* args)
+{
+    int err, max;
+
+    if (!PyArg_ParseTuple(args, "i:set_lk_max", &max))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_lk_max(self->db_env, max);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+#endif
+
+
+
+static PyObject*
+DBEnv_set_lk_max_locks(DBEnvObject* self, PyObject* args)
+{
+    int err, max;
+
+    if (!PyArg_ParseTuple(args, "i:set_lk_max_locks", &max))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_lk_max_locks(self->db_env, max);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_get_lk_max_locks(DBEnvObject* self)
+{
+    int err;
+    u_int32_t lk_max;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_lk_max_locks(self->db_env, &lk_max);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(lk_max);
+}
+
+static PyObject*
+DBEnv_set_lk_max_lockers(DBEnvObject* self, PyObject* args)
+{
+    int err, max;
+
+    if (!PyArg_ParseTuple(args, "i:set_lk_max_lockers", &max))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_lk_max_lockers(self->db_env, max);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_get_lk_max_lockers(DBEnvObject* self)
+{
+    int err;
+    u_int32_t lk_max;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_lk_max_lockers(self->db_env, &lk_max);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(lk_max);
+}
+
+static PyObject*
+DBEnv_set_lk_max_objects(DBEnvObject* self, PyObject* args)
+{
+    int err, max;
+
+    if (!PyArg_ParseTuple(args, "i:set_lk_max_objects", &max))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_lk_max_objects(self->db_env, max);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_get_lk_max_objects(DBEnvObject* self)
+{
+    int err;
+    u_int32_t lk_max;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_lk_max_objects(self->db_env, &lk_max);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(lk_max);
+}
+
+static PyObject*
+DBEnv_get_mp_mmapsize(DBEnvObject* self)
+{
+    int err;
+    size_t mmapsize;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_mp_mmapsize(self->db_env, &mmapsize);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(mmapsize);
+}
+
+static PyObject*
+DBEnv_set_mp_mmapsize(DBEnvObject* self, PyObject* args)
+{
+    int err, mp_mmapsize;
+
+    if (!PyArg_ParseTuple(args, "i:set_mp_mmapsize", &mp_mmapsize))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_mp_mmapsize(self->db_env, mp_mmapsize);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+
+static PyObject*
+DBEnv_set_tmp_dir(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    char *dir;
+
+    if (!PyArg_ParseTuple(args, "s:set_tmp_dir", &dir))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_tmp_dir(self->db_env, dir);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_get_tmp_dir(DBEnvObject* self)
+{
+    int err;
+    const char *dirpp;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_tmp_dir(self->db_env, &dirpp);
+    MYDB_END_ALLOW_THREADS;
+
+    RETURN_IF_ERR();
+
+    return PyBytes_FromString(dirpp);
+}
+
+static PyObject*
+DBEnv_txn_recover(DBEnvObject* self)
+{
+    int flags = DB_FIRST;
+    int err, i;
+    PyObject *list, *tuple, *gid;
+    DBTxnObject *txn;
+#define PREPLIST_LEN 16
+    DB_PREPLIST preplist[PREPLIST_LEN];
+#if (DBVER < 48) || (DBVER >= 52)
+    long retp;
+#else
+    u_int32_t retp;
+#endif
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    list=PyList_New(0);
+    if (!list)
+        return NULL;
+    while (!0) {
+        MYDB_BEGIN_ALLOW_THREADS
+        err=self->db_env->txn_recover(self->db_env,
+                        preplist, PREPLIST_LEN, &retp, flags);
+#undef PREPLIST_LEN
+        MYDB_END_ALLOW_THREADS
+        if (err) {
+            Py_DECREF(list);
+            RETURN_IF_ERR();
+        }
+        if (!retp) break;
+        flags=DB_NEXT;  /* Prepare for next loop pass */
+        for (i=0; i<retp; i++) {
+            gid=PyBytes_FromStringAndSize((char *)(preplist[i].gid),
+                                DB_GID_SIZE);
+            if (!gid) {
+                Py_DECREF(list);
+                return NULL;
+            }
+            txn=newDBTxnObject(self, NULL, preplist[i].txn, 0);
+            if (!txn) {
+                Py_DECREF(list);
+                Py_DECREF(gid);
+                return NULL;
+            }
+            txn->flag_prepare=1;  /* Recover state */
+            tuple=PyTuple_New(2);
+            if (!tuple) {
+                Py_DECREF(list);
+                Py_DECREF(gid);
+                Py_DECREF(txn);
+                return NULL;
+            }
+            if (PyTuple_SetItem(tuple, 0, gid)) {
+                Py_DECREF(list);
+                Py_DECREF(gid);
+                Py_DECREF(txn);
+                Py_DECREF(tuple);
+                return NULL;
+            }
+            if (PyTuple_SetItem(tuple, 1, (PyObject *)txn)) {
+                Py_DECREF(list);
+                Py_DECREF(txn);
+                Py_DECREF(tuple); /* This delete the "gid" also */
+                return NULL;
+            }
+            if (PyList_Append(list, tuple)) {
+                Py_DECREF(list);
+                Py_DECREF(tuple);/* This delete the "gid" and the "txn" also */
+                return NULL;
+            }
+            Py_DECREF(tuple);
+        }
+    }
+    return list;
+}
+
+static PyObject*
+DBEnv_txn_begin(DBEnvObject* self, PyObject* args, PyObject* kwargs)
+{
+    int flags = 0;
+    PyObject* txnobj = NULL;
+    DB_TXN *txn = NULL;
+    static char* kwnames[] = { "parent", "flags", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|Oi:txn_begin", kwnames,
+                                     &txnobj, &flags))
+        return NULL;
+
+    if (!checkTxnObj(txnobj, &txn))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    return (PyObject*)newDBTxnObject(self, (DBTxnObject *)txnobj, NULL, flags);
+}
+
+
+static PyObject*
+DBEnv_txn_checkpoint(DBEnvObject* self, PyObject* args)
+{
+    int err, kbyte=0, min=0, flags=0;
+
+    if (!PyArg_ParseTuple(args, "|iii:txn_checkpoint", &kbyte, &min, &flags))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->txn_checkpoint(self->db_env, kbyte, min, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_get_tx_max(DBEnvObject* self)
+{
+    int err;
+    u_int32_t max;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_tx_max(self->db_env, &max);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return PyLong_FromUnsignedLong(max);
+}
+
+static PyObject*
+DBEnv_set_tx_max(DBEnvObject* self, PyObject* args)
+{
+    int err, max;
+
+    if (!PyArg_ParseTuple(args, "i:set_tx_max", &max))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_tx_max(self->db_env, max);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_get_tx_timestamp(DBEnvObject* self)
+{
+    int err;
+    time_t timestamp;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_tx_timestamp(self->db_env, &timestamp);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(timestamp);
+}
+
+static PyObject*
+DBEnv_set_tx_timestamp(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    long stamp;
+    time_t timestamp;
+
+    if (!PyArg_ParseTuple(args, "l:set_tx_timestamp", &stamp))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+    timestamp = (time_t)stamp;
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_tx_timestamp(self->db_env, &timestamp);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+
+static PyObject*
+DBEnv_lock_detect(DBEnvObject* self, PyObject* args)
+{
+    int err, atype, flags=0;
+    int aborted = 0;
+
+    if (!PyArg_ParseTuple(args, "i|i:lock_detect", &atype, &flags))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->lock_detect(self->db_env, flags, atype, &aborted);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(aborted);
+}
+
+
+static PyObject*
+DBEnv_lock_get(DBEnvObject* self, PyObject* args)
+{
+    int flags=0;
+    int locker, lock_mode;
+    DBT obj;
+    PyObject* objobj;
+
+    if (!PyArg_ParseTuple(args, "iOi|i:lock_get", &locker, &objobj, &lock_mode, &flags))
+        return NULL;
+
+
+    if (!make_dbt(objobj, &obj))
+        return NULL;
+
+    return (PyObject*)newDBLockObject(self, locker, &obj, lock_mode, flags);
+}
+
+
+static PyObject*
+DBEnv_lock_id(DBEnvObject* self)
+{
+    int err;
+    u_int32_t theID;
+
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->lock_id(self->db_env, &theID);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+
+    return NUMBER_FromLong((long)theID);
+}
+
+static PyObject*
+DBEnv_lock_id_free(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    u_int32_t theID;
+
+    if (!PyArg_ParseTuple(args, "I:lock_id_free", &theID))
+        return NULL;
+
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->lock_id_free(self->db_env, theID);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_lock_put(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    DBLockObject* dblockobj;
+
+    if (!PyArg_ParseTuple(args, "O!:lock_put", &DBLock_Type, &dblockobj))
+        return NULL;
+
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->lock_put(self->db_env, &dblockobj->lock);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+#if (DBVER >= 44)
+static PyObject*
+DBEnv_fileid_reset(DBEnvObject* self, PyObject* args, PyObject* kwargs)
+{
+    int err;
+    char *file;
+    u_int32_t flags = 0;
+    static char* kwnames[] = { "file", "flags", NULL};
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "z|i:fileid_reset", kwnames,
+                                     &file, &flags))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->fileid_reset(self->db_env, file, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_lsn_reset(DBEnvObject* self, PyObject* args, PyObject* kwargs)
+{
+    int err;
+    char *file;
+    u_int32_t flags = 0;
+    static char* kwnames[] = { "file", "flags", NULL};
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "z|i:lsn_reset", kwnames,
+                                     &file, &flags))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->lsn_reset(self->db_env, file, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+#endif /* DBVER >= 4.4 */
+
+
+static PyObject*
+DBEnv_stat_print(DBEnvObject* self, PyObject* args, PyObject *kwargs)
+{
+    int err;
+    int flags=0;
+    static char* kwnames[] = { "flags", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i:stat_print",
+                kwnames, &flags))
+    {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->stat_print(self->db_env, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+
+static PyObject*
+DBEnv_log_stat(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    DB_LOG_STAT* statp = NULL;
+    PyObject* d = NULL;
+    u_int32_t flags = 0;
+
+    if (!PyArg_ParseTuple(args, "|i:log_stat", &flags))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->log_stat(self->db_env, &statp, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+
+    /* Turn the stat structure into a dictionary */
+    d = PyDict_New();
+    if (d == NULL) {
+        if (statp)
+            free(statp);
+        return NULL;
+    }
+
+#define MAKE_ENTRY(name)  _addIntToDict(d, #name, statp->st_##name)
+
+    MAKE_ENTRY(magic);
+    MAKE_ENTRY(version);
+    MAKE_ENTRY(mode);
+    MAKE_ENTRY(lg_bsize);
+#if (DBVER >= 44)
+    MAKE_ENTRY(lg_size);
+    MAKE_ENTRY(record);
+#endif
+    MAKE_ENTRY(w_mbytes);
+    MAKE_ENTRY(w_bytes);
+    MAKE_ENTRY(wc_mbytes);
+    MAKE_ENTRY(wc_bytes);
+    MAKE_ENTRY(wcount);
+    MAKE_ENTRY(wcount_fill);
+#if (DBVER >= 44)
+    MAKE_ENTRY(rcount);
+#endif
+    MAKE_ENTRY(scount);
+    MAKE_ENTRY(cur_file);
+    MAKE_ENTRY(cur_offset);
+    MAKE_ENTRY(disk_file);
+    MAKE_ENTRY(disk_offset);
+    MAKE_ENTRY(maxcommitperflush);
+    MAKE_ENTRY(mincommitperflush);
+    MAKE_ENTRY(regsize);
+    MAKE_ENTRY(region_wait);
+    MAKE_ENTRY(region_nowait);
+
+#undef MAKE_ENTRY
+    free(statp);
+    return d;
+} /* DBEnv_log_stat */
+
+
+static PyObject*
+DBEnv_log_stat_print(DBEnvObject* self, PyObject* args, PyObject *kwargs)
+{
+    int err;
+    int flags=0;
+    static char* kwnames[] = { "flags", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i:log_stat_print",
+                kwnames, &flags))
+    {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->log_stat_print(self->db_env, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+
+static PyObject*
+DBEnv_lock_stat(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    DB_LOCK_STAT* sp;
+    PyObject* d = NULL;
+    u_int32_t flags = 0;
+
+    if (!PyArg_ParseTuple(args, "|i:lock_stat", &flags))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->lock_stat(self->db_env, &sp, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+
+    /* Turn the stat structure into a dictionary */
+    d = PyDict_New();
+    if (d == NULL) {
+        free(sp);
+        return NULL;
+    }
+
+#define MAKE_ENTRY(name)  _addIntToDict(d, #name, sp->st_##name)
+
+    MAKE_ENTRY(id);
+    MAKE_ENTRY(cur_maxid);
+    MAKE_ENTRY(nmodes);
+    MAKE_ENTRY(maxlocks);
+    MAKE_ENTRY(maxlockers);
+    MAKE_ENTRY(maxobjects);
+    MAKE_ENTRY(nlocks);
+    MAKE_ENTRY(maxnlocks);
+    MAKE_ENTRY(nlockers);
+    MAKE_ENTRY(maxnlockers);
+    MAKE_ENTRY(nobjects);
+    MAKE_ENTRY(maxnobjects);
+    MAKE_ENTRY(nrequests);
+    MAKE_ENTRY(nreleases);
+#if (DBVER >= 44)
+    MAKE_ENTRY(nupgrade);
+    MAKE_ENTRY(ndowngrade);
+#endif
+#if (DBVER < 44)
+    MAKE_ENTRY(nnowaits);       /* these were renamed in 4.4 */
+    MAKE_ENTRY(nconflicts);
+#else
+    MAKE_ENTRY(lock_nowait);
+    MAKE_ENTRY(lock_wait);
+#endif
+    MAKE_ENTRY(ndeadlocks);
+    MAKE_ENTRY(locktimeout);
+    MAKE_ENTRY(txntimeout);
+    MAKE_ENTRY(nlocktimeouts);
+    MAKE_ENTRY(ntxntimeouts);
+#if (DBVER >= 46)
+    MAKE_ENTRY(objs_wait);
+    MAKE_ENTRY(objs_nowait);
+    MAKE_ENTRY(lockers_wait);
+    MAKE_ENTRY(lockers_nowait);
+#if (DBVER >= 47)
+    MAKE_ENTRY(lock_wait);
+    MAKE_ENTRY(lock_nowait);
+#else
+    MAKE_ENTRY(locks_wait);
+    MAKE_ENTRY(locks_nowait);
+#endif
+    MAKE_ENTRY(hash_len);
+#endif
+    MAKE_ENTRY(regsize);
+    MAKE_ENTRY(region_wait);
+    MAKE_ENTRY(region_nowait);
+
+#undef MAKE_ENTRY
+    free(sp);
+    return d;
+}
+
+static PyObject*
+DBEnv_lock_stat_print(DBEnvObject* self, PyObject* args, PyObject *kwargs)
+{
+    int err;
+    int flags=0;
+    static char* kwnames[] = { "flags", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i:lock_stat_print",
+                kwnames, &flags))
+    {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->lock_stat_print(self->db_env, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+
+static PyObject*
+DBEnv_log_cursor(DBEnvObject* self)
+{
+    int err;
+    DB_LOGC* dblogc;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->log_cursor(self->db_env, &dblogc, 0);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return (PyObject*) newDBLogCursorObject(dblogc, self);
+}
+
+
+static PyObject*
+DBEnv_log_flush(DBEnvObject* self)
+{
+    int err;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS
+    err = self->db_env->log_flush(self->db_env, NULL);
+    MYDB_END_ALLOW_THREADS
+
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_log_file(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    DB_LSN lsn = {0, 0};
+    int size = 20;
+    char *name = NULL;
+    PyObject *retval;
+
+    if (!PyArg_ParseTuple(args, "(ii):log_file", &lsn.file, &lsn.offset))
+        return NULL;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    do {
+        name = malloc(size);
+        if (!name) {
+            PyErr_NoMemory();
+            return NULL;
+        }
+        MYDB_BEGIN_ALLOW_THREADS;
+        err = self->db_env->log_file(self->db_env, &lsn, name, size);
+        MYDB_END_ALLOW_THREADS;
+        if (err == EINVAL) {
+            free(name);
+            size *= 2;
+        } else if (err) {
+            free(name);
+            RETURN_IF_ERR();
+            assert(0);  /* Unreachable... supposely */
+            return NULL;
+        }
+/*
+** If the final buffer we try is too small, we will
+** get this exception:
+** DBInvalidArgError:
+**    (22, 'Invalid argument -- DB_ENV->log_file: name buffer is too short')
+*/
+    } while ((err == EINVAL) && (size<(1<<17)));
+
+    RETURN_IF_ERR();  /* Maybe the size is not the problem */
+
+    retval = Py_BuildValue("s", name);
+    free(name);
+    return retval;
+}
+
+
+#if (DBVER >= 44)
+static PyObject*
+DBEnv_log_printf(DBEnvObject* self, PyObject* args, PyObject *kwargs)
+{
+    int err;
+    char *string;
+    PyObject *txnobj = NULL;
+    DB_TXN *txn = NULL;
+    static char* kwnames[] = {"string", "txn", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|O:log_printf", kwnames,
+                &string, &txnobj))
+        return NULL;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    if (!checkTxnObj(txnobj, &txn))
+        return NULL;
+
+    /*
+    ** Do not use the format string directly, to avoid attacks.
+    */
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->log_printf(self->db_env, txn, "%s", string);
+    MYDB_END_ALLOW_THREADS;
+
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+#endif
+
+
+static PyObject*
+DBEnv_log_archive(DBEnvObject* self, PyObject* args)
+{
+    int flags=0;
+    int err;
+    char **log_list = NULL;
+    PyObject* list;
+    PyObject* item = NULL;
+
+    if (!PyArg_ParseTuple(args, "|i:log_archive", &flags))
+        return NULL;
+
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->log_archive(self->db_env, &log_list, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+
+    list = PyList_New(0);
+    if (list == NULL) {
+        if (log_list)
+            free(log_list);
+        return NULL;
+    }
+
+    if (log_list) {
+        char **log_list_start;
+        for (log_list_start = log_list; *log_list != NULL; ++log_list) {
+            item = PyBytes_FromString (*log_list);
+            if (item == NULL) {
+                Py_DECREF(list);
+                list = NULL;
+                break;
+            }
+            if (PyList_Append(list, item)) {
+                Py_DECREF(list);
+                list = NULL;
+                Py_DECREF(item);
+                break;
+            }
+            Py_DECREF(item);
+        }
+        free(log_list_start);
+    }
+    return list;
+}
+
+
+#if (DBVER >= 52)
+static PyObject*
+DBEnv_repmgr_site(DBEnvObject* self, PyObject* args, PyObject *kwargs)
+{
+    int err;
+    DB_SITE* site;
+    char *host;
+    u_int port;
+    static char* kwnames[] = {"host", "port", NULL};
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "si:repmgr_site", kwnames,
+                                     &host, &port))
+        return NULL;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->repmgr_site(self->db_env, host, port, &site, 0);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return (PyObject*) newDBSiteObject(site, self);
+}
+
+static PyObject*
+DBEnv_repmgr_site_by_eid(DBEnvObject* self, PyObject* args, PyObject *kwargs)
+{
+    int err;
+    DB_SITE* site;
+    int eid;
+    static char* kwnames[] = {"eid", NULL};
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "i:repmgr_site_by_eid",
+                kwnames, &eid))
+        return NULL;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->repmgr_site_by_eid(self->db_env, eid, &site);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return (PyObject*) newDBSiteObject(site, self);
+}
+#endif
+
+
+#if (DBVER >= 44)
+static PyObject*
+DBEnv_mutex_stat(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    DB_MUTEX_STAT* statp = NULL;
+    PyObject* d = NULL;
+    u_int32_t flags = 0;
+
+    if (!PyArg_ParseTuple(args, "|i:mutex_stat", &flags))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->mutex_stat(self->db_env, &statp, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+
+    /* Turn the stat structure into a dictionary */
+    d = PyDict_New();
+    if (d == NULL) {
+        if (statp)
+            free(statp);
+        return NULL;
+    }
+
+#define MAKE_ENTRY(name)  _addIntToDict(d, #name, statp->st_##name)
+
+    MAKE_ENTRY(mutex_align);
+    MAKE_ENTRY(mutex_tas_spins);
+    MAKE_ENTRY(mutex_cnt);
+    MAKE_ENTRY(mutex_free);
+    MAKE_ENTRY(mutex_inuse);
+    MAKE_ENTRY(mutex_inuse_max);
+    MAKE_ENTRY(regsize);
+    MAKE_ENTRY(region_wait);
+    MAKE_ENTRY(region_nowait);
+
+#undef MAKE_ENTRY
+    free(statp);
+    return d;
+}
+#endif
+
+
+#if (DBVER >= 44)
+static PyObject*
+DBEnv_mutex_stat_print(DBEnvObject* self, PyObject* args, PyObject *kwargs)
+{
+    int err;
+    int flags=0;
+    static char* kwnames[] = { "flags", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i:mutex_stat_print",
+                kwnames, &flags))
+    {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->mutex_stat_print(self->db_env, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+#endif
+
+
+static PyObject*
+DBEnv_txn_stat_print(DBEnvObject* self, PyObject* args, PyObject *kwargs)
+{
+    int err;
+    int flags=0;
+    static char* kwnames[] = { "flags", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i:stat_print",
+                kwnames, &flags))
+    {
+        return NULL;
+    }
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->txn_stat_print(self->db_env, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+
+static PyObject*
+DBEnv_txn_stat(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    DB_TXN_STAT* sp;
+    PyObject* d = NULL;
+    u_int32_t flags=0;
+
+    if (!PyArg_ParseTuple(args, "|i:txn_stat", &flags))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->txn_stat(self->db_env, &sp, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+
+    /* Turn the stat structure into a dictionary */
+    d = PyDict_New();
+    if (d == NULL) {
+        free(sp);
+        return NULL;
+    }
+
+#define MAKE_ENTRY(name)        _addIntToDict(d, #name, sp->st_##name)
+#define MAKE_TIME_T_ENTRY(name) _addTimeTToDict(d, #name, sp->st_##name)
+#define MAKE_DB_LSN_ENTRY(name) _addDB_lsnToDict(d, #name, sp->st_##name)
+
+    MAKE_DB_LSN_ENTRY(last_ckp);
+    MAKE_TIME_T_ENTRY(time_ckp);
+    MAKE_ENTRY(last_txnid);
+    MAKE_ENTRY(maxtxns);
+    MAKE_ENTRY(nactive);
+    MAKE_ENTRY(maxnactive);
+#if (DBVER >= 45)
+    MAKE_ENTRY(nsnapshot);
+    MAKE_ENTRY(maxnsnapshot);
+#endif
+    MAKE_ENTRY(nbegins);
+    MAKE_ENTRY(naborts);
+    MAKE_ENTRY(ncommits);
+    MAKE_ENTRY(nrestores);
+    MAKE_ENTRY(regsize);
+    MAKE_ENTRY(region_wait);
+    MAKE_ENTRY(region_nowait);
+
+#undef MAKE_DB_LSN_ENTRY
+#undef MAKE_ENTRY
+#undef MAKE_TIME_T_ENTRY
+    free(sp);
+    return d;
+}
+
+
+static PyObject*
+DBEnv_set_get_returns_none(DBEnvObject* self, PyObject* args)
+{
+    int flags=0;
+    int oldValue=0;
+
+    if (!PyArg_ParseTuple(args,"i:set_get_returns_none", &flags))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    if (self->moduleFlags.getReturnsNone)
+        ++oldValue;
+    if (self->moduleFlags.cursorSetReturnsNone)
+        ++oldValue;
+    self->moduleFlags.getReturnsNone = (flags >= 1);
+    self->moduleFlags.cursorSetReturnsNone = (flags >= 2);
+    return NUMBER_FromLong(oldValue);
+}
+
+static PyObject*
+DBEnv_get_private(DBEnvObject* self)
+{
+    /* We can give out the private field even if dbenv is closed */
+    Py_INCREF(self->private_obj);
+    return self->private_obj;
+}
+
+static PyObject*
+DBEnv_set_private(DBEnvObject* self, PyObject* private_obj)
+{
+    /* We can set the private field even if dbenv is closed */
+    Py_DECREF(self->private_obj);
+    Py_INCREF(private_obj);
+    self->private_obj = private_obj;
+    RETURN_NONE();
+}
+
+#if (DBVER >= 47)
+static PyObject*
+DBEnv_set_intermediate_dir_mode(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    const char *mode;
+
+    if (!PyArg_ParseTuple(args,"s:set_intermediate_dir_mode", &mode))
+        return NULL;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_intermediate_dir_mode(self->db_env, mode);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_get_intermediate_dir_mode(DBEnvObject* self)
+{
+    int err;
+    const char *mode;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_intermediate_dir_mode(self->db_env, &mode);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return Py_BuildValue("s", mode);
+}
+#endif
+
+#if (DBVER < 47)
+static PyObject*
+DBEnv_set_intermediate_dir(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    int mode;
+    u_int32_t flags;
+
+    if (!PyArg_ParseTuple(args, "iI:set_intermediate_dir", &mode, &flags))
+        return NULL;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_intermediate_dir(self->db_env, mode, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+#endif
+
+static PyObject*
+DBEnv_get_open_flags(DBEnvObject* self)
+{
+    int err;
+    unsigned int flags;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_open_flags(self->db_env, &flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(flags);
+}
+
+#if (DBVER < 48)
+static PyObject*
+DBEnv_set_rpc_server(DBEnvObject* self, PyObject* args, PyObject* kwargs)
+{
+    int err;
+    char *host;
+    long cl_timeout=0, sv_timeout=0;
+
+    static char* kwnames[] = { "host", "cl_timeout", "sv_timeout", NULL};
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|ll:set_rpc_server", kwnames,
+                                     &host, &cl_timeout, &sv_timeout))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_rpc_server(self->db_env, NULL, host, cl_timeout,
+            sv_timeout, 0);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+#endif
+
+static PyObject*
+DBEnv_set_mp_max_openfd(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    int maxopenfd;
+
+    if (!PyArg_ParseTuple(args, "i:set_mp_max_openfd", &maxopenfd)) {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_mp_max_openfd(self->db_env, maxopenfd);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_get_mp_max_openfd(DBEnvObject* self)
+{
+    int err;
+    int maxopenfd;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_mp_max_openfd(self->db_env, &maxopenfd);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(maxopenfd);
+}
+
+
+static PyObject*
+DBEnv_set_mp_max_write(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    int maxwrite, maxwrite_sleep;
+
+    if (!PyArg_ParseTuple(args, "ii:set_mp_max_write", &maxwrite,
+                &maxwrite_sleep)) {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_mp_max_write(self->db_env, maxwrite,
+            maxwrite_sleep);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_get_mp_max_write(DBEnvObject* self)
+{
+    int err;
+    int maxwrite;
+#if (DBVER >= 46)
+    db_timeout_t maxwrite_sleep;
+#else
+    int maxwrite_sleep;
+#endif
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_mp_max_write(self->db_env, &maxwrite,
+            &maxwrite_sleep);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+
+    return Py_BuildValue("(ii)", maxwrite, (int)maxwrite_sleep);
+}
+
+
+static PyObject*
+DBEnv_set_verbose(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    int which, onoff;
+
+    if (!PyArg_ParseTuple(args, "ii:set_verbose", &which, &onoff)) {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_verbose(self->db_env, which, onoff);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_get_verbose(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    int which;
+    int verbose;
+
+    if (!PyArg_ParseTuple(args, "i:get_verbose", &which)) {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->get_verbose(self->db_env, which, &verbose);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return PyBool_FromLong(verbose);
+}
+
+#if (DBVER >= 45)
+static void
+_dbenv_event_notifyCallback(DB_ENV* db_env, u_int32_t event, void *event_info)
+{
+    DBEnvObject *dbenv;
+    PyObject* callback;
+    PyObject* args;
+    PyObject* result = NULL;
+
+    MYDB_BEGIN_BLOCK_THREADS;
+    dbenv = (DBEnvObject *)db_env->app_private;
+    callback = dbenv->event_notifyCallback;
+    if (callback) {
+        if (event == DB_EVENT_REP_NEWMASTER) {
+            args = Py_BuildValue("(Oii)", dbenv, event, *((int *)event_info));
+        } else {
+            args = Py_BuildValue("(OiO)", dbenv, event, Py_None);
+        }
+        if (args) {
+            result = PyEval_CallObject(callback, args);
+        }
+        if ((!args) || (!result)) {
+            PyErr_Print();
+        }
+        Py_XDECREF(args);
+        Py_XDECREF(result);
+    }
+    MYDB_END_BLOCK_THREADS;
+}
+#endif
+
+#if (DBVER >= 45)
+static PyObject*
+DBEnv_set_event_notify(DBEnvObject* self, PyObject* notifyFunc)
+{
+    int err;
+
+    CHECK_ENV_NOT_CLOSED(self);
+
+    if (!PyCallable_Check(notifyFunc)) {
+            makeTypeError("Callable", notifyFunc);
+            return NULL;
+    }
+
+    Py_XDECREF(self->event_notifyCallback);
+    Py_INCREF(notifyFunc);
+    self->event_notifyCallback = notifyFunc;
+
+    /* This is to workaround a problem with un-initialized threads (see
+       comment in DB_associate) */
+#ifdef WITH_THREAD
+    PyEval_InitThreads();
+#endif
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->set_event_notify(self->db_env, _dbenv_event_notifyCallback);
+    MYDB_END_ALLOW_THREADS;
+
+    if (err) {
+            Py_DECREF(notifyFunc);
+            self->event_notifyCallback = NULL;
+    }
 
-#if (DBVER < 41)
-    MAKE_ENTRY(lastid);
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
 #endif
-    MAKE_ENTRY(nmodes);
-#if (DBVER >= 32)
-    MAKE_ENTRY(maxlocks);
-    MAKE_ENTRY(maxlockers);
-    MAKE_ENTRY(maxobjects);
-    MAKE_ENTRY(nlocks);
-    MAKE_ENTRY(maxnlocks);
+
+
+/* --------------------------------------------------------------------- */
+/* REPLICATION METHODS: Base Replication */
+
+
+static PyObject*
+DBEnv_rep_process_message(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    PyObject *control_py, *rec_py;
+    DBT control, rec;
+    int envid;
+    DB_LSN lsn;
+
+    if (!PyArg_ParseTuple(args, "OOi:rep_process_message", &control_py,
+                &rec_py, &envid))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    if (!make_dbt(control_py, &control))
+        return NULL;
+    if (!make_dbt(rec_py, &rec))
+        return NULL;
+
+    MYDB_BEGIN_ALLOW_THREADS;
+#if (DBVER >= 46)
+    err = self->db_env->rep_process_message(self->db_env, &control, &rec,
+            envid, &lsn);
+#else
+    err = self->db_env->rep_process_message(self->db_env, &control, &rec,
+            &envid, &lsn);
 #endif
-    MAKE_ENTRY(nlockers);
-    MAKE_ENTRY(maxnlockers);
-#if (DBVER >= 32)
-    MAKE_ENTRY(nobjects);
-    MAKE_ENTRY(maxnobjects);
+    MYDB_END_ALLOW_THREADS;
+    switch (err) {
+        case DB_REP_NEWMASTER :
+          return Py_BuildValue("(iO)", envid, Py_None);
+          break;
+
+        case DB_REP_DUPMASTER :
+        case DB_REP_HOLDELECTION :
+#if (DBVER >= 44)
+        case DB_REP_IGNORE :
+        case DB_REP_JOIN_FAILURE :
 #endif
-    MAKE_ENTRY(nrequests);
-    MAKE_ENTRY(nreleases);
-#if (DBVER < 44)
-    MAKE_ENTRY(nnowaits);       /* these were renamed in 4.4 */
-    MAKE_ENTRY(nconflicts);
+            return Py_BuildValue("(iO)", err, Py_None);
+            break;
+        case DB_REP_NEWSITE :
+            {
+                PyObject *tmp, *r;
+
+                if (!(tmp = PyBytes_FromStringAndSize(rec.data, rec.size))) {
+                    return NULL;
+                }
+
+                r = Py_BuildValue("(iO)", err, tmp);
+                Py_DECREF(tmp);
+                return r;
+                break;
+            }
+        case DB_REP_NOTPERM :
+        case DB_REP_ISPERM :
+            return Py_BuildValue("(i(ll))", err, lsn.file, lsn.offset);
+            break;
+    }
+    RETURN_IF_ERR();
+    return PyTuple_Pack(2, Py_None, Py_None);
+}
+
+static int
+_DBEnv_rep_transportCallback(DB_ENV* db_env, const DBT* control, const DBT* rec,
+        const DB_LSN *lsn, int envid, u_int32_t flags)
+{
+    DBEnvObject *dbenv;
+    PyObject* rep_transport;
+    PyObject* args;
+    PyObject *a, *b;
+    PyObject* result = NULL;
+    int ret=0;
+
+    MYDB_BEGIN_BLOCK_THREADS;
+    dbenv = (DBEnvObject *)db_env->app_private;
+    rep_transport = dbenv->rep_transport;
+
+    /*
+    ** The errors in 'a' or 'b' are detected in "Py_BuildValue".
+    */
+    a = PyBytes_FromStringAndSize(control->data, control->size);
+    b = PyBytes_FromStringAndSize(rec->data, rec->size);
+
+    args = Py_BuildValue(
+            "(OOO(ll)iI)",
+            dbenv,
+            a, b,
+            lsn->file, lsn->offset, envid, flags);
+    if (args) {
+        result = PyEval_CallObject(rep_transport, args);
+    }
+
+    if ((!args) || (!result)) {
+        PyErr_Print();
+        ret = -1;
+    }
+    Py_XDECREF(a);
+    Py_XDECREF(b);
+    Py_XDECREF(args);
+    Py_XDECREF(result);
+    MYDB_END_BLOCK_THREADS;
+    return ret;
+}
+
+static PyObject*
+DBEnv_rep_set_transport(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    int envid;
+    PyObject *rep_transport;
+
+    if (!PyArg_ParseTuple(args, "iO:rep_set_transport", &envid, &rep_transport))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+    if (!PyCallable_Check(rep_transport)) {
+        makeTypeError("Callable", rep_transport);
+        return NULL;
+    }
+
+    MYDB_BEGIN_ALLOW_THREADS;
+#if (DBVER >=45)
+    err = self->db_env->rep_set_transport(self->db_env, envid,
+            &_DBEnv_rep_transportCallback);
 #else
-    MAKE_ENTRY(lock_nowait);
-    MAKE_ENTRY(lock_wait);
+    err = self->db_env->set_rep_transport(self->db_env, envid,
+            &_DBEnv_rep_transportCallback);
 #endif
-    MAKE_ENTRY(ndeadlocks);
-    MAKE_ENTRY(regsize);
-    MAKE_ENTRY(region_wait);
-    MAKE_ENTRY(region_nowait);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+
+    Py_DECREF(self->rep_transport);
+    Py_INCREF(rep_transport);
+    self->rep_transport = rep_transport;
+    RETURN_NONE();
+}
+
+#if (DBVER >= 47)
+static PyObject*
+DBEnv_rep_set_request(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    unsigned int minimum, maximum;
+
+    if (!PyArg_ParseTuple(args,"II:rep_set_request", &minimum, &maximum))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_set_request(self->db_env, minimum, maximum);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_rep_get_request(DBEnvObject* self)
+{
+    int err;
+    u_int32_t minimum, maximum;
+
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_get_request(self->db_env, &minimum, &maximum);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return Py_BuildValue("II", minimum, maximum);
+}
+#endif
+
+#if (DBVER >= 45)
+static PyObject*
+DBEnv_rep_set_limit(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    int limit;
+
+    if (!PyArg_ParseTuple(args,"i:rep_set_limit", &limit))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_set_limit(self->db_env, 0, limit);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_rep_get_limit(DBEnvObject* self)
+{
+    int err;
+    u_int32_t gbytes, bytes;
+
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_get_limit(self->db_env, &gbytes, &bytes);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(bytes);
+}
+#endif
+
+#if (DBVER >= 44)
+static PyObject*
+DBEnv_rep_set_config(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    int which;
+    int onoff;
+
+    if (!PyArg_ParseTuple(args,"ii:rep_set_config", &which, &onoff))
+        return NULL;
+    CHECK_ENV_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_set_config(self->db_env, which, onoff);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_rep_get_config(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    int which;
+    int onoff;
+
+    if (!PyArg_ParseTuple(args, "i:rep_get_config", &which)) {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_get_config(self->db_env, which, &onoff);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return PyBool_FromLong(onoff);
+}
+#endif
+
+#if (DBVER >= 46)
+static PyObject*
+DBEnv_rep_elect(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    u_int32_t nsites, nvotes;
+
+    if (!PyArg_ParseTuple(args, "II:rep_elect", &nsites, &nvotes)) {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_elect(self->db_env, nsites, nvotes, 0);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+#endif
+
+static PyObject*
+DBEnv_rep_start(DBEnvObject* self, PyObject* args, PyObject* kwargs)
+{
+    int err;
+    PyObject *cdata_py = Py_None;
+    DBT cdata;
+    int flags;
+    static char* kwnames[] = {"flags","cdata", NULL};
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs,
+                "i|O:rep_start", kwnames, &flags, &cdata_py))
+    {
+            return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+
+    if (!make_dbt(cdata_py, &cdata))
+        return NULL;
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_start(self->db_env, cdata.size ? &cdata : NULL,
+            flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+#if (DBVER >= 44)
+static PyObject*
+DBEnv_rep_sync(DBEnvObject* self)
+{
+    int err;
+
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_sync(self->db_env, 0);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+#endif
+
+
+#if (DBVER >= 45)
+static PyObject*
+DBEnv_rep_set_nsites(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    int nsites;
+
+    if (!PyArg_ParseTuple(args, "i:rep_set_nsites", &nsites)) {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_set_nsites(self->db_env, nsites);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_rep_get_nsites(DBEnvObject* self)
+{
+    int err;
+#if (DBVER >= 47)
+    u_int32_t nsites;
+#else
+    int nsites;
+#endif
+
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_get_nsites(self->db_env, &nsites);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(nsites);
+}
+
+static PyObject*
+DBEnv_rep_set_priority(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    int priority;
+
+    if (!PyArg_ParseTuple(args, "i:rep_set_priority", &priority)) {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_set_priority(self->db_env, priority);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_rep_get_priority(DBEnvObject* self)
+{
+    int err;
+#if (DBVER >= 47)
+    u_int32_t priority;
+#else
+    int priority;
+#endif
+
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_get_priority(self->db_env, &priority);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(priority);
+}
 
-#undef MAKE_ENTRY
-    free(sp);
-    return d;
+static PyObject*
+DBEnv_rep_set_timeout(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    int which, timeout;
+
+    if (!PyArg_ParseTuple(args, "ii:rep_set_timeout", &which, &timeout)) {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_set_timeout(self->db_env, which, timeout);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_rep_get_timeout(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    int which;
+    u_int32_t timeout;
+
+    if (!PyArg_ParseTuple(args, "i:rep_get_timeout", &which)) {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_get_timeout(self->db_env, which, &timeout);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(timeout);
 }
+#endif
 
 
+#if (DBVER >= 47)
 static PyObject*
-DBEnv_log_archive(DBEnvObject* self, PyObject* args)
+DBEnv_rep_set_clockskew(DBEnvObject* self, PyObject* args)
 {
-    int flags=0;
     int err;
-    char **log_list = NULL;
-    PyObject* list;
-    PyObject* item = NULL;
+    unsigned int fast, slow;
 
-    if (!PyArg_ParseTuple(args, "|i:log_archive", &flags))
+    if (!PyArg_ParseTuple(args,"II:rep_set_clockskew", &fast, &slow))
         return NULL;
 
     CHECK_ENV_NOT_CLOSED(self);
+
     MYDB_BEGIN_ALLOW_THREADS;
-#if (DBVER >= 40)
-    err = self->db_env->log_archive(self->db_env, &log_list, flags);
-#elif (DBVER == 33)
-    err = log_archive(self->db_env, &log_list, flags);
-#else
-    err = log_archive(self->db_env, &log_list, flags, NULL);
+    err = self->db_env->rep_set_clockskew(self->db_env, fast, slow);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_rep_get_clockskew(DBEnvObject* self)
+{
+    int err;
+    unsigned int fast, slow;
+
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_get_clockskew(self->db_env, &fast, &slow);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return Py_BuildValue("(II)", fast, slow);
+}
 #endif
+
+static PyObject*
+DBEnv_rep_stat_print(DBEnvObject* self, PyObject* args, PyObject *kwargs)
+{
+    int err;
+    int flags=0;
+    static char* kwnames[] = { "flags", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i:rep_stat_print",
+                kwnames, &flags))
+    {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_stat_print(self->db_env, flags);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
+    RETURN_NONE();
+}
 
-    list = PyList_New(0);
-    if (list == NULL) {
-        if (log_list)
-            free(log_list);
+static PyObject*
+DBEnv_rep_stat(DBEnvObject* self, PyObject* args, PyObject *kwargs)
+{
+    int err;
+    int flags=0;
+    DB_REP_STAT *statp;
+    PyObject *stats;
+    static char* kwnames[] = { "flags", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i:rep_stat",
+                kwnames, &flags))
+    {
         return NULL;
     }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->rep_stat(self->db_env, &statp, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
 
-    if (log_list) {
-        char **log_list_start;
-        for (log_list_start = log_list; *log_list != NULL; ++log_list) {
-            item = PyString_FromString (*log_list);
-            if (item == NULL) {
-                Py_DECREF(list);
-                list = NULL;
-                break;
-            }
-            PyList_Append(list, item);
-            Py_DECREF(item);
-        }
-        free(log_list_start);
+    stats=PyDict_New();
+    if (stats == NULL) {
+        free(statp);
+        return NULL;
     }
-    return list;
+
+#define MAKE_ENTRY(name)  _addIntToDict(stats, #name, statp->st_##name)
+#define MAKE_DB_LSN_ENTRY(name) _addDB_lsnToDict(stats , #name, statp->st_##name)
+
+#if (DBVER >= 44)
+    MAKE_ENTRY(bulk_fills);
+    MAKE_ENTRY(bulk_overflows);
+    MAKE_ENTRY(bulk_records);
+    MAKE_ENTRY(bulk_transfers);
+    MAKE_ENTRY(client_rerequests);
+    MAKE_ENTRY(client_svc_miss);
+    MAKE_ENTRY(client_svc_req);
+#endif
+    MAKE_ENTRY(dupmasters);
+    MAKE_ENTRY(egen);
+    MAKE_ENTRY(election_nvotes);
+    MAKE_ENTRY(startup_complete);
+    MAKE_ENTRY(pg_duplicated);
+    MAKE_ENTRY(pg_records);
+    MAKE_ENTRY(pg_requested);
+    MAKE_ENTRY(next_pg);
+    MAKE_ENTRY(waiting_pg);
+    MAKE_ENTRY(election_cur_winner);
+    MAKE_ENTRY(election_gen);
+    MAKE_DB_LSN_ENTRY(election_lsn);
+    MAKE_ENTRY(election_nsites);
+    MAKE_ENTRY(election_priority);
+#if (DBVER >= 44)
+    MAKE_ENTRY(election_sec);
+    MAKE_ENTRY(election_usec);
+#endif
+    MAKE_ENTRY(election_status);
+    MAKE_ENTRY(election_tiebreaker);
+    MAKE_ENTRY(election_votes);
+    MAKE_ENTRY(elections);
+    MAKE_ENTRY(elections_won);
+    MAKE_ENTRY(env_id);
+    MAKE_ENTRY(env_priority);
+    MAKE_ENTRY(gen);
+    MAKE_ENTRY(log_duplicated);
+    MAKE_ENTRY(log_queued);
+    MAKE_ENTRY(log_queued_max);
+    MAKE_ENTRY(log_queued_total);
+    MAKE_ENTRY(log_records);
+    MAKE_ENTRY(log_requested);
+    MAKE_ENTRY(master);
+    MAKE_ENTRY(master_changes);
+#if (DBVER >= 47)
+    MAKE_ENTRY(max_lease_sec);
+    MAKE_ENTRY(max_lease_usec);
+    MAKE_DB_LSN_ENTRY(max_perm_lsn);
+#endif
+    MAKE_ENTRY(msgs_badgen);
+    MAKE_ENTRY(msgs_processed);
+    MAKE_ENTRY(msgs_recover);
+    MAKE_ENTRY(msgs_send_failures);
+    MAKE_ENTRY(msgs_sent);
+    MAKE_ENTRY(newsites);
+    MAKE_DB_LSN_ENTRY(next_lsn);
+    MAKE_ENTRY(nsites);
+    MAKE_ENTRY(nthrottles);
+    MAKE_ENTRY(outdated);
+#if (DBVER >= 46)
+    MAKE_ENTRY(startsync_delayed);
+#endif
+    MAKE_ENTRY(status);
+    MAKE_ENTRY(txns_applied);
+    MAKE_DB_LSN_ENTRY(waiting_lsn);
+
+#undef MAKE_DB_LSN_ENTRY
+#undef MAKE_ENTRY
+
+    free(statp);
+    return stats;
 }
 
+/* --------------------------------------------------------------------- */
+/* REPLICATION METHODS: Replication Manager */
 
+#if (DBVER >= 45)
 static PyObject*
-DBEnv_txn_stat(DBEnvObject* self, PyObject* args)
+DBEnv_repmgr_start(DBEnvObject* self, PyObject* args, PyObject*
+        kwargs)
 {
     int err;
-    DB_TXN_STAT* sp;
-    PyObject* d = NULL;
-    u_int32_t flags=0;
+    int nthreads, flags;
+    static char* kwnames[] = {"nthreads","flags", NULL};
 
-    if (!PyArg_ParseTuple(args, "|i:txn_stat", &flags))
-        return NULL;
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs,
+                "ii:repmgr_start", kwnames, &nthreads, &flags))
+    {
+            return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->repmgr_start(self->db_env, nthreads, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+#if (DBVER < 52)
+static PyObject*
+DBEnv_repmgr_set_local_site(DBEnvObject* self, PyObject* args, PyObject*
+        kwargs)
+{
+    int err;
+    char *host;
+    int port;
+    int flags = 0;
+    static char* kwnames[] = {"host", "port", "flags", NULL};
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs,
+                "si|i:repmgr_set_local_site", kwnames, &host, &port, &flags))
+    {
+            return NULL;
+    }
     CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->repmgr_set_local_site(self->db_env, host, port, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_repmgr_add_remote_site(DBEnvObject* self, PyObject* args, PyObject*
+        kwargs)
+{
+    int err;
+    char *host;
+    int port;
+    int flags = 0;
+    int eidp;
+    static char* kwnames[] = {"host", "port", "flags", NULL};
 
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs,
+                "si|i:repmgr_add_remote_site", kwnames, &host, &port, &flags))
+    {
+            return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
     MYDB_BEGIN_ALLOW_THREADS;
-#if (DBVER >= 40)
-    err = self->db_env->txn_stat(self->db_env, &sp, flags);
-#elif (DBVER == 33)
-    err = txn_stat(self->db_env, &sp);
-#else
-    err = txn_stat(self->db_env, &sp, NULL);
+    err = self->db_env->repmgr_add_remote_site(self->db_env, host, port, &eidp, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    return NUMBER_FromLong(eidp);
+}
 #endif
+
+static PyObject*
+DBEnv_repmgr_set_ack_policy(DBEnvObject* self, PyObject* args)
+{
+    int err;
+    int ack_policy;
+
+    if (!PyArg_ParseTuple(args, "i:repmgr_set_ack_policy", &ack_policy))
+    {
+            return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->repmgr_set_ack_policy(self->db_env, ack_policy);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
+static PyObject*
+DBEnv_repmgr_get_ack_policy(DBEnvObject* self)
+{
+    int err;
+    int ack_policy;
+
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->repmgr_get_ack_policy(self->db_env, &ack_policy);
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
+    return NUMBER_FromLong(ack_policy);
+}
 
-    /* Turn the stat structure into a dictionary */
-    d = PyDict_New();
-    if (d == NULL) {
-        free(sp);
+static PyObject*
+DBEnv_repmgr_site_list(DBEnvObject* self)
+{
+    int err;
+    unsigned int countp;
+    DB_REPMGR_SITE *listp;
+    PyObject *stats, *key, *tuple;
+
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->repmgr_site_list(self->db_env, &countp, &listp);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+
+    stats=PyDict_New();
+    if (stats == NULL) {
+        free(listp);
         return NULL;
     }
 
-#define MAKE_ENTRY(name)  _addIntToDict(d, #name, sp->st_##name)
+    for(;countp--;) {
+        key=NUMBER_FromLong(listp[countp].eid);
+        if(!key) {
+            Py_DECREF(stats);
+            free(listp);
+            return NULL;
+        }
+        tuple=Py_BuildValue("(sII)", listp[countp].host,
+                listp[countp].port, listp[countp].status);
+        if(!tuple) {
+            Py_DECREF(key);
+            Py_DECREF(stats);
+            free(listp);
+            return NULL;
+        }
+        if(PyDict_SetItem(stats, key, tuple)) {
+            Py_DECREF(key);
+            Py_DECREF(tuple);
+            Py_DECREF(stats);
+            free(listp);
+            return NULL;
+        }
+        Py_DECREF(key);
+        Py_DECREF(tuple);
+    }
+    free(listp);
+    return stats;
+}
+#endif
 
-    MAKE_ENTRY(time_ckp);
-    MAKE_ENTRY(last_txnid);
-    MAKE_ENTRY(maxtxns);
-    MAKE_ENTRY(nactive);
-    MAKE_ENTRY(maxnactive);
-    MAKE_ENTRY(nbegins);
-    MAKE_ENTRY(naborts);
-    MAKE_ENTRY(ncommits);
-    MAKE_ENTRY(regsize);
-    MAKE_ENTRY(region_wait);
-    MAKE_ENTRY(region_nowait);
+#if (DBVER >= 46)
+static PyObject*
+DBEnv_repmgr_stat_print(DBEnvObject* self, PyObject* args, PyObject *kwargs)
+{
+    int err;
+    int flags=0;
+    static char* kwnames[] = { "flags", NULL };
 
-#undef MAKE_ENTRY
-    free(sp);
-    return d;
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i:repmgr_stat_print",
+                kwnames, &flags))
+    {
+        return NULL;
+    }
+    CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->repmgr_stat_print(self->db_env, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
 }
 
-
 static PyObject*
-DBEnv_set_get_returns_none(DBEnvObject* self, PyObject* args)
+DBEnv_repmgr_stat(DBEnvObject* self, PyObject* args, PyObject *kwargs)
 {
+    int err;
     int flags=0;
-    int oldValue=0;
+    DB_REPMGR_STAT *statp;
+    PyObject *stats;
+    static char* kwnames[] = { "flags", NULL };
 
-    if (!PyArg_ParseTuple(args,"i:set_get_returns_none", &flags))
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i:repmgr_stat",
+                kwnames, &flags))
+    {
         return NULL;
+    }
     CHECK_ENV_NOT_CLOSED(self);
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->db_env->repmgr_stat(self->db_env, &statp, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
 
-    if (self->moduleFlags.getReturnsNone)
-        ++oldValue;
-    if (self->moduleFlags.cursorSetReturnsNone)
-        ++oldValue;
-    self->moduleFlags.getReturnsNone = (flags >= 1);
-    self->moduleFlags.cursorSetReturnsNone = (flags >= 2);
-    return PyInt_FromLong(oldValue);
+    stats=PyDict_New();
+    if (stats == NULL) {
+        free(statp);
+        return NULL;
+    }
+
+#define MAKE_ENTRY(name)  _addIntToDict(stats, #name, statp->st_##name)
+
+    MAKE_ENTRY(perm_failed);
+    MAKE_ENTRY(msgs_queued);
+    MAKE_ENTRY(msgs_dropped);
+    MAKE_ENTRY(connection_drop);
+    MAKE_ENTRY(connect_fail);
+
+#undef MAKE_ENTRY
+
+    free(statp);
+    return stats;
+}
+#endif
+
+
+/* --------------------------------------------------------------------- */
+/* DBTxn methods */
+
+
+static void _close_transaction_cursors(DBTxnObject* txn)
+{
+    PyObject *dummy;
+
+    while(txn->children_cursors) {
+        PyErr_Warn(PyExc_RuntimeWarning,
+            "Must close cursors before resolving a transaction.");
+        dummy=DBC_close_internal(txn->children_cursors);
+        Py_XDECREF(dummy);
+    }
 }
 
+static void _promote_transaction_dbs_and_sequences(DBTxnObject *txn)
+{
+    DBObject *db;
+    DBSequenceObject *dbs;
 
-/* --------------------------------------------------------------------- */
-/* DBTxn methods */
+    while (txn->children_dbs) {
+        db=txn->children_dbs;
+        EXTRACT_FROM_DOUBLE_LINKED_LIST_TXN(db);
+        if (txn->parent_txn) {
+            INSERT_IN_DOUBLE_LINKED_LIST_TXN(txn->parent_txn->children_dbs,db);
+            db->txn=txn->parent_txn;
+        } else {
+            /* The db is already linked to its environment,
+            ** so nothing to do.
+            */
+            db->txn=NULL;
+        }
+    }
+
+    while (txn->children_sequences) {
+        dbs=txn->children_sequences;
+        EXTRACT_FROM_DOUBLE_LINKED_LIST_TXN(dbs);
+        if (txn->parent_txn) {
+            INSERT_IN_DOUBLE_LINKED_LIST_TXN(txn->parent_txn->children_sequences,dbs);
+            dbs->txn=txn->parent_txn;
+        } else {
+            /* The sequence is already linked to its
+            ** parent db. Nothing to do.
+            */
+            dbs->txn=NULL;
+        }
+    }
+}
 
 
 static PyObject*
@@ -4706,22 +8132,30 @@
     if (!PyArg_ParseTuple(args, "|i:commit", &flags))
         return NULL;
 
+    _close_transaction_cursors(self);
+
     if (!self->txn) {
         PyObject *t =  Py_BuildValue("(is)", 0, "DBTxn must not be used "
-                                     "after txn_commit or txn_abort");
-        PyErr_SetObject(DBError, t);
-        Py_DECREF(t);
+                                     "after txn_commit, txn_abort "
+                                     "or txn_discard");
+        if (t) {
+            PyErr_SetObject(DBError, t);
+            Py_DECREF(t);
+        }
         return NULL;
     }
+    self->flag_prepare=0;
     txn = self->txn;
     self->txn = NULL;   /* this DB_TXN is no longer valid after this call */
+
+    EXTRACT_FROM_DOUBLE_LINKED_LIST(self);
+
     MYDB_BEGIN_ALLOW_THREADS;
-#if (DBVER >= 40)
     err = txn->commit(txn, flags);
-#else
-    err = txn_commit(txn, flags);
-#endif
     MYDB_END_ALLOW_THREADS;
+
+    _promote_transaction_dbs_and_sequences(self);
+
     RETURN_IF_ERR();
     RETURN_NONE();
 }
@@ -4729,7 +8163,6 @@
 static PyObject*
 DBTxn_prepare(DBTxnObject* self, PyObject* args)
 {
-#if (DBVER >= 33)
     int err;
     char* gid=NULL;
     int   gid_size=0;
@@ -4737,129 +8170,239 @@
     if (!PyArg_ParseTuple(args, "s#:prepare", &gid, &gid_size))
         return NULL;
 
-    if (gid_size != DB_XIDDATASIZE) {
+    if (gid_size != DB_GID_SIZE) {
         PyErr_SetString(PyExc_TypeError,
-                        "gid must be DB_XIDDATASIZE bytes long");
+                        "gid must be DB_GID_SIZE bytes long");
         return NULL;
     }
 
     if (!self->txn) {
         PyObject *t = Py_BuildValue("(is)", 0,"DBTxn must not be used "
-                                    "after txn_commit or txn_abort");
-        PyErr_SetObject(DBError, t);
-        Py_DECREF(t);
+                                    "after txn_commit, txn_abort "
+                                    "or txn_discard");
+        if (t) {
+            PyErr_SetObject(DBError, t);
+            Py_DECREF(t);
+        }
         return NULL;
     }
+    self->flag_prepare=1;  /* Prepare state */
     MYDB_BEGIN_ALLOW_THREADS;
-#if (DBVER >= 40)
     err = self->txn->prepare(self->txn, (u_int8_t*)gid);
-#else
-    err = txn_prepare(self->txn, (u_int8_t*)gid);
-#endif
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
     RETURN_NONE();
-#else
-    int err;
+}
 
-    if (!PyArg_ParseTuple(args, ":prepare"))
-        return NULL;
+
+static PyObject*
+DBTxn_abort_discard_internal(DBTxnObject* self, int discard)
+{
+    PyObject *dummy;
+    int err=0;
+    DB_TXN *txn;
 
     if (!self->txn) {
         PyObject *t = Py_BuildValue("(is)", 0, "DBTxn must not be used "
-                                    "after txn_commit or txn_abort");
-        PyErr_SetObject(DBError, t);
-        Py_DECREF(t);
+                                    "after txn_commit, txn_abort "
+                                    "or txn_discard");
+        if (t) {
+            PyErr_SetObject(DBError, t);
+            Py_DECREF(t);
+        }
         return NULL;
     }
+    txn = self->txn;
+    self->txn = NULL;   /* this DB_TXN is no longer valid after this call */
+
+    _close_transaction_cursors(self);
+    while (self->children_sequences) {
+        dummy=DBSequence_close_internal(self->children_sequences,0,0);
+        Py_XDECREF(dummy);
+    }
+    while (self->children_dbs) {
+        dummy=DB_close_internal(self->children_dbs, 0, 0);
+        Py_XDECREF(dummy);
+    }
+
+    EXTRACT_FROM_DOUBLE_LINKED_LIST(self);
+
     MYDB_BEGIN_ALLOW_THREADS;
-    err = txn_prepare(self->txn);
+    if (discard) {
+        assert(!self->flag_prepare);
+        err = txn->discard(txn,0);
+    } else {
+        /*
+        ** If the transaction is in the "prepare" or "recover" state,
+        ** we better do not implicitly abort it.
+        */
+        if (!self->flag_prepare) {
+            err = txn->abort(txn);
+        }
+    }
     MYDB_END_ALLOW_THREADS;
     RETURN_IF_ERR();
     RETURN_NONE();
-#endif
 }
 
+static PyObject*
+DBTxn_abort(DBTxnObject* self)
+{
+    self->flag_prepare=0;
+    _close_transaction_cursors(self);
+
+    return DBTxn_abort_discard_internal(self,0);
+}
 
 static PyObject*
-DBTxn_abort(DBTxnObject* self, PyObject* args)
+DBTxn_discard(DBTxnObject* self)
 {
-    int err;
-    DB_TXN *txn;
+    self->flag_prepare=0;
+    _close_transaction_cursors(self);
 
-    if (!PyArg_ParseTuple(args, ":abort"))
-        return NULL;
+    return DBTxn_abort_discard_internal(self,1);
+}
+
+
+static PyObject*
+DBTxn_id(DBTxnObject* self)
+{
+    int id;
 
     if (!self->txn) {
         PyObject *t = Py_BuildValue("(is)", 0, "DBTxn must not be used "
-                                    "after txn_commit or txn_abort");
-        PyErr_SetObject(DBError, t);
-        Py_DECREF(t);
+                                    "after txn_commit, txn_abort "
+                                    "or txn_discard");
+        if (t) {
+            PyErr_SetObject(DBError, t);
+            Py_DECREF(t);
+        }
         return NULL;
     }
-    txn = self->txn;
-    self->txn = NULL;   /* this DB_TXN is no longer valid after this call */
     MYDB_BEGIN_ALLOW_THREADS;
-#if (DBVER >= 40)
-    err = txn->abort(txn);
-#else
-    err = txn_abort(txn);
-#endif
+    id = self->txn->id(self->txn);
+    MYDB_END_ALLOW_THREADS;
+    return NUMBER_FromLong(id);
+}
+
+
+static PyObject*
+DBTxn_set_timeout(DBTxnObject* self, PyObject* args, PyObject* kwargs)
+{
+    int err;
+    u_int32_t flags=0;
+    u_int32_t timeout = 0;
+    static char* kwnames[] = { "timeout", "flags", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ii:set_timeout", kwnames,
+                &timeout, &flags)) {
+        return NULL;
+    }
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->txn->set_timeout(self->txn, (db_timeout_t)timeout, flags);
     MYDB_END_ALLOW_THREADS;
+
     RETURN_IF_ERR();
     RETURN_NONE();
 }
 
 
+#if (DBVER >= 44)
 static PyObject*
-DBTxn_id(DBTxnObject* self, PyObject* args)
+DBTxn_set_name(DBTxnObject* self, PyObject* args)
 {
-    int id;
+    int err;
+    const char *name;
 
-    if (!PyArg_ParseTuple(args, ":id"))
+    if (!PyArg_ParseTuple(args, "s:set_name", &name))
         return NULL;
 
-    if (!self->txn) {
-        PyObject *t = Py_BuildValue("(is)", 0, "DBTxn must not be used "
-                                    "after txn_commit or txn_abort");
-        PyErr_SetObject(DBError, t);
-        Py_DECREF(t);
-        return NULL;
-    }
     MYDB_BEGIN_ALLOW_THREADS;
-#if (DBVER >= 40)
-    id = self->txn->id(self->txn);
-#else
-    id = txn_id(self->txn);
+    err = self->txn->set_name(self->txn, name);
+    MYDB_END_ALLOW_THREADS;
+
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
 #endif
+
+
+#if (DBVER >= 44)
+static PyObject*
+DBTxn_get_name(DBTxnObject* self)
+{
+    int err;
+    const char *name;
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->txn->get_name(self->txn, &name);
     MYDB_END_ALLOW_THREADS;
-    return PyInt_FromLong(id);
+
+    RETURN_IF_ERR();
+#if (PY_VERSION_HEX < 0x03000000)
+    if (!name) {
+        return PyString_FromString("");
+    }
+    return PyString_FromString(name);
+#else
+    if (!name) {
+        return PyUnicode_FromString("");
+    }
+    return PyUnicode_FromString(name);
+#endif
 }
+#endif
+
 
-#if (DBVER >= 43)
 /* --------------------------------------------------------------------- */
 /* DBSequence methods */
 
 
 static PyObject*
-DBSequence_close(DBSequenceObject* self, PyObject* args)
+DBSequence_close_internal(DBSequenceObject* self, int flags, int do_not_close)
 {
-    int err, flags=0;
-    if (!PyArg_ParseTuple(args,"|i:close", &flags))
-        return NULL;
-    CHECK_SEQUENCE_NOT_CLOSED(self)
+    int err=0;
 
-    MYDB_BEGIN_ALLOW_THREADS
-    err = self->sequence->close(self->sequence, flags);
-    self->sequence = NULL;
-    MYDB_END_ALLOW_THREADS
+    if (self->sequence!=NULL) {
+        EXTRACT_FROM_DOUBLE_LINKED_LIST(self);
+        if (self->txn) {
+            EXTRACT_FROM_DOUBLE_LINKED_LIST_TXN(self);
+            self->txn=NULL;
+        }
 
-    RETURN_IF_ERR();
+        /*
+        ** "do_not_close" is used to dispose all related objects in the
+        ** tree, without actually releasing the "root" object.
+        ** This is done, for example, because function calls like
+        ** "DBSequence.remove()" implicitly close the underlying handle. So
+        ** the handle doesn't need to be closed, but related objects
+        ** must be cleaned up.
+        */
+        if (!do_not_close) {
+            MYDB_BEGIN_ALLOW_THREADS
+            err = self->sequence->close(self->sequence, flags);
+            MYDB_END_ALLOW_THREADS
+        }
+        self->sequence = NULL;
+
+        RETURN_IF_ERR();
+    }
 
     RETURN_NONE();
 }
 
 static PyObject*
+DBSequence_close(DBSequenceObject* self, PyObject* args)
+{
+    int flags=0;
+    if (!PyArg_ParseTuple(args,"|i:close", &flags))
+        return NULL;
+
+    return DBSequence_close_internal(self,flags,0);
+}
+
+static PyObject*
 DBSequence_get(DBSequenceObject* self, PyObject* args, PyObject* kwargs)
 {
     int err, flags = 0;
@@ -4881,25 +8424,23 @@
 
     RETURN_IF_ERR();
     return PyLong_FromLongLong(value);
-
 }
 
 static PyObject*
-DBSequence_get_dbp(DBSequenceObject* self, PyObject* args)
+DBSequence_get_dbp(DBSequenceObject* self)
 {
-    if (!PyArg_ParseTuple(args,":get_dbp"))
-        return NULL;
     CHECK_SEQUENCE_NOT_CLOSED(self)
     Py_INCREF(self->mydb);
     return (PyObject* )self->mydb;
 }
 
 static PyObject*
-DBSequence_get_key(DBSequenceObject* self, PyObject* args)
+DBSequence_get_key(DBSequenceObject* self)
 {
     int err;
     DBT key;
-    PyObject *retval;
+    PyObject *retval = NULL;
+
     key.flags = DB_DBT_MALLOC;
     CHECK_SEQUENCE_NOT_CLOSED(self)
     MYDB_BEGIN_ALLOW_THREADS
@@ -4907,7 +8448,7 @@
     MYDB_END_ALLOW_THREADS
 
     if (!err)
-        retval = PyString_FromStringAndSize(key.data, key.size); 
+        retval = Build_PyString(key.data, key.size);
 
     FREE_DBT(key);
     RETURN_IF_ERR();
@@ -4916,16 +8457,18 @@
 }
 
 static PyObject*
-DBSequence_init_value(DBSequenceObject* self, PyObject* args)
+DBSequence_initial_value(DBSequenceObject* self, PyObject* args)
 {
     int err;
-    db_seq_t value;
-    if (!PyArg_ParseTuple(args,"L:init_value", &value))
+    PY_LONG_LONG value;
+    db_seq_t value2;
+    if (!PyArg_ParseTuple(args,"L:initial_value", &value))
         return NULL;
     CHECK_SEQUENCE_NOT_CLOSED(self)
 
+    value2=value; /* If truncation, compiler should show a warning */
     MYDB_BEGIN_ALLOW_THREADS
-    err = self->sequence->initial_value(self->sequence, value);
+    err = self->sequence->initial_value(self->sequence, value2);
     MYDB_END_ALLOW_THREADS
 
     RETURN_IF_ERR();
@@ -4956,15 +8499,21 @@
     err = self->sequence->open(self->sequence, txn, &key, flags);
     MYDB_END_ALLOW_THREADS
 
-    CLEAR_DBT(key);
+    FREE_DBT(key);
     RETURN_IF_ERR();
 
+    if (txn) {
+        INSERT_IN_DOUBLE_LINKED_LIST_TXN(((DBTxnObject *)txnobj)->children_sequences,self);
+        self->txn=(DBTxnObject *)txnobj;
+    }
+
     RETURN_NONE();
 }
 
 static PyObject*
 DBSequence_remove(DBSequenceObject* self, PyObject* args, PyObject* kwargs)
 {
+    PyObject *dummy;
     int err, flags = 0;
     PyObject *txnobj = NULL;
     DB_TXN *txn = NULL;
@@ -4982,6 +8531,9 @@
     err = self->sequence->remove(self->sequence, txn, flags);
     MYDB_END_ALLOW_THREADS
 
+    dummy=DBSequence_close_internal(self,flags,1);
+    Py_XDECREF(dummy);
+
     RETURN_IF_ERR();
     RETURN_NONE();
 }
@@ -5003,11 +8555,10 @@
 }
 
 static PyObject*
-DBSequence_get_cachesize(DBSequenceObject* self, PyObject* args)
+DBSequence_get_cachesize(DBSequenceObject* self)
 {
     int err, size;
-    if (!PyArg_ParseTuple(args,":get_cachesize"))
-        return NULL;
+
     CHECK_SEQUENCE_NOT_CLOSED(self)
 
     MYDB_BEGIN_ALLOW_THREADS
@@ -5015,7 +8566,7 @@
     MYDB_END_ALLOW_THREADS
 
     RETURN_IF_ERR();
-    return PyInt_FromLong(size);
+    return NUMBER_FromLong(size);
 }
 
 static PyObject*
@@ -5032,16 +8583,14 @@
 
     RETURN_IF_ERR();
     RETURN_NONE();
-
 }
 
 static PyObject*
-DBSequence_get_flags(DBSequenceObject* self, PyObject* args)
+DBSequence_get_flags(DBSequenceObject* self)
 {
     unsigned int flags;
     int err;
-    if (!PyArg_ParseTuple(args,":get_flags"))
-        return NULL;
+
     CHECK_SEQUENCE_NOT_CLOSED(self)
 
     MYDB_BEGIN_ALLOW_THREADS
@@ -5049,20 +8598,23 @@
     MYDB_END_ALLOW_THREADS
 
     RETURN_IF_ERR();
-    return PyInt_FromLong((int)flags);
+    return NUMBER_FromLong((int)flags);
 }
 
 static PyObject*
 DBSequence_set_range(DBSequenceObject* self, PyObject* args)
 {
     int err;
-    db_seq_t min, max;
+    PY_LONG_LONG min, max;
+    db_seq_t min2, max2;
     if (!PyArg_ParseTuple(args,"(LL):set_range", &min, &max))
         return NULL;
     CHECK_SEQUENCE_NOT_CLOSED(self)
 
+    min2=min;  /* If truncation, compiler should show a warning */
+    max2=max;
     MYDB_BEGIN_ALLOW_THREADS
-    err = self->sequence->set_range(self->sequence, min, max);
+    err = self->sequence->set_range(self->sequence, min2, max2);
     MYDB_END_ALLOW_THREADS
 
     RETURN_IF_ERR();
@@ -5070,22 +8622,47 @@
 }
 
 static PyObject*
-DBSequence_get_range(DBSequenceObject* self, PyObject* args)
+DBSequence_get_range(DBSequenceObject* self)
 {
     int err;
-    db_seq_t min, max;
-    if (!PyArg_ParseTuple(args,":get_range"))
-        return NULL;
+    PY_LONG_LONG min, max;
+    db_seq_t min2, max2;
+
     CHECK_SEQUENCE_NOT_CLOSED(self)
 
     MYDB_BEGIN_ALLOW_THREADS
-    err = self->sequence->get_range(self->sequence, &min, &max);
+    err = self->sequence->get_range(self->sequence, &min2, &max2);
     MYDB_END_ALLOW_THREADS
 
     RETURN_IF_ERR();
+    min=min2;  /* If truncation, compiler should show a warning */
+    max=max2;
     return Py_BuildValue("(LL)", min, max);
 }
 
+
+static PyObject*
+DBSequence_stat_print(DBSequenceObject* self, PyObject* args, PyObject *kwargs)
+{
+    int err;
+    int flags=0;
+    static char* kwnames[] = { "flags", NULL };
+
+    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|i:stat_print",
+                kwnames, &flags))
+    {
+        return NULL;
+    }
+
+    CHECK_SEQUENCE_NOT_CLOSED(self);
+
+    MYDB_BEGIN_ALLOW_THREADS;
+    err = self->sequence->stat_print(self->sequence, flags);
+    MYDB_END_ALLOW_THREADS;
+    RETURN_IF_ERR();
+    RETURN_NONE();
+}
+
 static PyObject*
 DBSequence_stat(DBSequenceObject* self, PyObject* args, PyObject* kwargs)
 {
@@ -5127,36 +8704,36 @@
     free(sp);
     return dict_stat;
 }
-#endif
 
 
 /* --------------------------------------------------------------------- */
 /* Method definition tables and type objects */
 
 static PyMethodDef DB_methods[] = {
-    {"append",          (PyCFunction)DB_append,         METH_VARARGS},
-#if (DBVER >= 33)
+    {"append",          (PyCFunction)DB_append,         METH_VARARGS|METH_KEYWORDS},
     {"associate",       (PyCFunction)DB_associate,      METH_VARARGS|METH_KEYWORDS},
-#endif
     {"close",           (PyCFunction)DB_close,          METH_VARARGS},
-#if (DBVER >= 32)
+#if (DBVER >= 47)
+    {"compact",         (PyCFunction)DB_compact,        METH_VARARGS|METH_KEYWORDS},
+#endif
     {"consume",         (PyCFunction)DB_consume,        METH_VARARGS|METH_KEYWORDS},
     {"consume_wait",    (PyCFunction)DB_consume_wait,   METH_VARARGS|METH_KEYWORDS},
-#endif
     {"cursor",          (PyCFunction)DB_cursor,         METH_VARARGS|METH_KEYWORDS},
     {"delete",          (PyCFunction)DB_delete,         METH_VARARGS|METH_KEYWORDS},
-    {"fd",              (PyCFunction)DB_fd,             METH_VARARGS},
+    {"fd",              (PyCFunction)DB_fd,             METH_NOARGS},
+#if (DBVER >= 46)
+    {"exists",          (PyCFunction)DB_exists,
+        METH_VARARGS|METH_KEYWORDS},
+#endif
     {"get",             (PyCFunction)DB_get,            METH_VARARGS|METH_KEYWORDS},
-#if (DBVER >= 33)
     {"pget",            (PyCFunction)DB_pget,           METH_VARARGS|METH_KEYWORDS},
-#endif
     {"get_both",        (PyCFunction)DB_get_both,       METH_VARARGS|METH_KEYWORDS},
-    {"get_byteswapped", (PyCFunction)DB_get_byteswapped,METH_VARARGS},
+    {"get_byteswapped", (PyCFunction)DB_get_byteswapped,METH_NOARGS},
     {"get_size",        (PyCFunction)DB_get_size,       METH_VARARGS|METH_KEYWORDS},
-    {"get_type",        (PyCFunction)DB_get_type,       METH_VARARGS},
+    {"get_type",        (PyCFunction)DB_get_type,       METH_NOARGS},
     {"join",            (PyCFunction)DB_join,           METH_VARARGS},
     {"key_range",       (PyCFunction)DB_key_range,      METH_VARARGS|METH_KEYWORDS},
-    {"has_key",         (PyCFunction)DB_has_key,        METH_VARARGS},
+    {"has_key",         (PyCFunction)DB_has_key,        METH_VARARGS|METH_KEYWORDS},
     {"items",           (PyCFunction)DB_items,          METH_VARARGS},
     {"keys",            (PyCFunction)DB_keys,           METH_VARARGS},
     {"open",            (PyCFunction)DB_open,           METH_VARARGS|METH_KEYWORDS},
@@ -5164,31 +8741,48 @@
     {"remove",          (PyCFunction)DB_remove,         METH_VARARGS|METH_KEYWORDS},
     {"rename",          (PyCFunction)DB_rename,         METH_VARARGS},
     {"set_bt_minkey",   (PyCFunction)DB_set_bt_minkey,  METH_VARARGS},
-#if (DBVER >= 33)
-    {"set_bt_compare",  (PyCFunction)DB_set_bt_compare, METH_VARARGS},
-#endif
+    {"get_bt_minkey",   (PyCFunction)DB_get_bt_minkey,  METH_NOARGS},
+    {"set_bt_compare",  (PyCFunction)DB_set_bt_compare, METH_O},
     {"set_cachesize",   (PyCFunction)DB_set_cachesize,  METH_VARARGS},
-#if (DBVER >= 41)
+    {"get_cachesize",   (PyCFunction)DB_get_cachesize,  METH_NOARGS},
+    {"set_dup_compare", (PyCFunction)DB_set_dup_compare, METH_O},
     {"set_encrypt",     (PyCFunction)DB_set_encrypt,    METH_VARARGS|METH_KEYWORDS},
-#endif
+    {"get_encrypt_flags", (PyCFunction)DB_get_encrypt_flags, METH_NOARGS},
     {"set_flags",       (PyCFunction)DB_set_flags,      METH_VARARGS},
+    {"get_flags",       (PyCFunction)DB_get_flags,      METH_NOARGS},
+    {"get_transactional", (PyCFunction)DB_get_transactional, METH_NOARGS},
     {"set_h_ffactor",   (PyCFunction)DB_set_h_ffactor,  METH_VARARGS},
+    {"get_h_ffactor",   (PyCFunction)DB_get_h_ffactor,  METH_NOARGS},
     {"set_h_nelem",     (PyCFunction)DB_set_h_nelem,    METH_VARARGS},
+    {"get_h_nelem",     (PyCFunction)DB_get_h_nelem,    METH_NOARGS},
     {"set_lorder",      (PyCFunction)DB_set_lorder,     METH_VARARGS},
+    {"get_lorder",      (PyCFunction)DB_get_lorder,     METH_NOARGS},
     {"set_pagesize",    (PyCFunction)DB_set_pagesize,   METH_VARARGS},
+    {"get_pagesize",    (PyCFunction)DB_get_pagesize,   METH_NOARGS},
     {"set_re_delim",    (PyCFunction)DB_set_re_delim,   METH_VARARGS},
+    {"get_re_delim",    (PyCFunction)DB_get_re_delim,   METH_NOARGS},
     {"set_re_len",      (PyCFunction)DB_set_re_len,     METH_VARARGS},
+    {"get_re_len",      (PyCFunction)DB_get_re_len,     METH_NOARGS},
     {"set_re_pad",      (PyCFunction)DB_set_re_pad,     METH_VARARGS},
+    {"get_re_pad",      (PyCFunction)DB_get_re_pad,     METH_NOARGS},
     {"set_re_source",   (PyCFunction)DB_set_re_source,  METH_VARARGS},
-#if (DBVER >= 32)
-    {"set_q_extentsize",(PyCFunction)DB_set_q_extentsize,METH_VARARGS},
+    {"get_re_source",   (PyCFunction)DB_get_re_source,  METH_NOARGS},
+    {"set_q_extentsize",(PyCFunction)DB_set_q_extentsize, METH_VARARGS},
+    {"get_q_extentsize",(PyCFunction)DB_get_q_extentsize, METH_NOARGS},
+    {"set_private",     (PyCFunction)DB_set_private,    METH_O},
+    {"get_private",     (PyCFunction)DB_get_private,    METH_NOARGS},
+#if (DBVER >= 46)
+    {"set_priority",    (PyCFunction)DB_set_priority,   METH_VARARGS},
+    {"get_priority",    (PyCFunction)DB_get_priority,   METH_NOARGS},
 #endif
+    {"get_dbname",      (PyCFunction)DB_get_dbname,     METH_NOARGS},
+    {"get_open_flags",  (PyCFunction)DB_get_open_flags, METH_NOARGS},
     {"stat",            (PyCFunction)DB_stat,           METH_VARARGS|METH_KEYWORDS},
+    {"stat_print",      (PyCFunction)DB_stat_print,
+        METH_VARARGS|METH_KEYWORDS},
     {"sync",            (PyCFunction)DB_sync,           METH_VARARGS},
-#if (DBVER >= 33)
     {"truncate",        (PyCFunction)DB_truncate,       METH_VARARGS|METH_KEYWORDS},
-#endif
-    {"type",            (PyCFunction)DB_get_type,       METH_VARARGS},
+    {"type",            (PyCFunction)DB_get_type,       METH_NOARGS},
     {"upgrade",         (PyCFunction)DB_upgrade,        METH_VARARGS},
     {"values",          (PyCFunction)DB_values,         METH_VARARGS},
     {"verify",          (PyCFunction)DB_verify,         METH_VARARGS|METH_KEYWORDS},
@@ -5197,6 +8791,20 @@
 };
 
 
+/* We need this to support __contains__() */
+static PySequenceMethods DB_sequence = {
+    0, /* sq_length, mapping wins here */
+    0, /* sq_concat */
+    0, /* sq_repeat */
+    0, /* sq_item */
+    0, /* sq_slice */
+    0, /* sq_ass_item */
+    0, /* sq_ass_slice */
+    (objobjproc)DB_contains, /* sq_contains */
+    0, /* sq_inplace_concat */
+    0, /* sq_inplace_repeat */
+};
+
 static PyMappingMethods DB_mapping = {
         DB_length,                   /*mp_length*/
         (binaryfunc)DB_subscript,    /*mp_subscript*/
@@ -5205,17 +8813,15 @@
 
 
 static PyMethodDef DBCursor_methods[] = {
-    {"close",           (PyCFunction)DBC_close,         METH_VARARGS},
+    {"close",           (PyCFunction)DBC_close,         METH_NOARGS},
     {"count",           (PyCFunction)DBC_count,         METH_VARARGS},
     {"current",         (PyCFunction)DBC_current,       METH_VARARGS|METH_KEYWORDS},
     {"delete",          (PyCFunction)DBC_delete,        METH_VARARGS},
     {"dup",             (PyCFunction)DBC_dup,           METH_VARARGS},
     {"first",           (PyCFunction)DBC_first,         METH_VARARGS|METH_KEYWORDS},
     {"get",             (PyCFunction)DBC_get,           METH_VARARGS|METH_KEYWORDS},
-#if (DBVER >= 33)
     {"pget",            (PyCFunction)DBC_pget,          METH_VARARGS|METH_KEYWORDS},
-#endif
-    {"get_recno",       (PyCFunction)DBC_get_recno,     METH_VARARGS},
+    {"get_recno",       (PyCFunction)DBC_get_recno,     METH_NOARGS},
     {"last",            (PyCFunction)DBC_last,          METH_VARARGS|METH_KEYWORDS},
     {"next",            (PyCFunction)DBC_next,          METH_VARARGS|METH_KEYWORDS},
     {"prev",            (PyCFunction)DBC_prev,          METH_VARARGS|METH_KEYWORDS},
@@ -5223,71 +8829,267 @@
     {"set",             (PyCFunction)DBC_set,           METH_VARARGS|METH_KEYWORDS},
     {"set_range",       (PyCFunction)DBC_set_range,     METH_VARARGS|METH_KEYWORDS},
     {"get_both",        (PyCFunction)DBC_get_both,      METH_VARARGS},
-    {"get_current_size",(PyCFunction)DBC_get_current_size, METH_VARARGS},
+    {"get_current_size",(PyCFunction)DBC_get_current_size, METH_NOARGS},
     {"set_both",        (PyCFunction)DBC_set_both,      METH_VARARGS},
     {"set_recno",       (PyCFunction)DBC_set_recno,     METH_VARARGS|METH_KEYWORDS},
     {"consume",         (PyCFunction)DBC_consume,       METH_VARARGS|METH_KEYWORDS},
     {"next_dup",        (PyCFunction)DBC_next_dup,      METH_VARARGS|METH_KEYWORDS},
     {"next_nodup",      (PyCFunction)DBC_next_nodup,    METH_VARARGS|METH_KEYWORDS},
+#if (DBVER >= 46)
+    {"prev_dup",        (PyCFunction)DBC_prev_dup,
+        METH_VARARGS|METH_KEYWORDS},
+#endif
     {"prev_nodup",      (PyCFunction)DBC_prev_nodup,    METH_VARARGS|METH_KEYWORDS},
     {"join_item",       (PyCFunction)DBC_join_item,     METH_VARARGS},
+#if (DBVER >= 46)
+    {"set_priority",    (PyCFunction)DBC_set_priority,
+        METH_VARARGS|METH_KEYWORDS},
+    {"get_priority",    (PyCFunction)DBC_get_priority, METH_NOARGS},
+#endif
+    {NULL,      NULL}       /* sentinel */
+};
+
+
+static PyMethodDef DBLogCursor_methods[] = {
+    {"close",   (PyCFunction)DBLogCursor_close,     METH_NOARGS},
+    {"current", (PyCFunction)DBLogCursor_current,   METH_NOARGS},
+    {"first",   (PyCFunction)DBLogCursor_first,     METH_NOARGS},
+    {"last",    (PyCFunction)DBLogCursor_last,      METH_NOARGS},
+    {"next",    (PyCFunction)DBLogCursor_next,      METH_NOARGS},
+    {"prev",    (PyCFunction)DBLogCursor_prev,      METH_NOARGS},
+    {"set",     (PyCFunction)DBLogCursor_set,       METH_VARARGS},
     {NULL,      NULL}       /* sentinel */
 };
 
+#if (DBVER >= 52)
+static PyMethodDef DBSite_methods[] = {
+    {"get_config",  (PyCFunction)DBSite_get_config,
+        METH_VARARGS | METH_KEYWORDS},
+    {"set_config",  (PyCFunction)DBSite_set_config,
+        METH_VARARGS | METH_KEYWORDS},
+    {"remove",      (PyCFunction)DBSite_remove,     METH_NOARGS},
+    {"get_eid",     (PyCFunction)DBSite_get_eid,    METH_NOARGS},
+    {"get_address", (PyCFunction)DBSite_get_address,    METH_NOARGS},
+    {"close",       (PyCFunction)DBSite_close,      METH_NOARGS},
+    {NULL,      NULL}       /* sentinel */
+};
+#endif
 
 static PyMethodDef DBEnv_methods[] = {
     {"close",           (PyCFunction)DBEnv_close,            METH_VARARGS},
     {"open",            (PyCFunction)DBEnv_open,             METH_VARARGS},
     {"remove",          (PyCFunction)DBEnv_remove,           METH_VARARGS},
-#if (DBVER >= 41)
     {"dbremove",        (PyCFunction)DBEnv_dbremove,         METH_VARARGS|METH_KEYWORDS},
     {"dbrename",        (PyCFunction)DBEnv_dbrename,         METH_VARARGS|METH_KEYWORDS},
+#if (DBVER >= 46)
+    {"set_thread_count", (PyCFunction)DBEnv_set_thread_count, METH_VARARGS},
+    {"get_thread_count", (PyCFunction)DBEnv_get_thread_count, METH_NOARGS},
+#endif
     {"set_encrypt",     (PyCFunction)DBEnv_set_encrypt,      METH_VARARGS|METH_KEYWORDS},
+    {"get_encrypt_flags", (PyCFunction)DBEnv_get_encrypt_flags, METH_NOARGS},
+    {"get_timeout",     (PyCFunction)DBEnv_get_timeout,
+        METH_VARARGS|METH_KEYWORDS},
+    {"set_timeout",     (PyCFunction)DBEnv_set_timeout,     METH_VARARGS|METH_KEYWORDS},
+    {"set_shm_key",     (PyCFunction)DBEnv_set_shm_key,     METH_VARARGS},
+    {"get_shm_key",     (PyCFunction)DBEnv_get_shm_key,     METH_NOARGS},
+#if (DBVER >= 46)
+    {"set_cache_max",   (PyCFunction)DBEnv_set_cache_max,   METH_VARARGS},
+    {"get_cache_max",   (PyCFunction)DBEnv_get_cache_max,   METH_NOARGS},
+#endif
+    {"set_cachesize",   (PyCFunction)DBEnv_set_cachesize,   METH_VARARGS},
+    {"get_cachesize",   (PyCFunction)DBEnv_get_cachesize,   METH_NOARGS},
+    {"memp_trickle",    (PyCFunction)DBEnv_memp_trickle,    METH_VARARGS},
+    {"memp_sync",       (PyCFunction)DBEnv_memp_sync,       METH_VARARGS},
+    {"memp_stat",       (PyCFunction)DBEnv_memp_stat,
+        METH_VARARGS|METH_KEYWORDS},
+    {"memp_stat_print", (PyCFunction)DBEnv_memp_stat_print,
+        METH_VARARGS|METH_KEYWORDS},
+#if (DBVER >= 44)
+    {"mutex_set_max",   (PyCFunction)DBEnv_mutex_set_max,   METH_VARARGS},
+    {"mutex_get_max",   (PyCFunction)DBEnv_mutex_get_max,   METH_NOARGS},
+    {"mutex_set_align", (PyCFunction)DBEnv_mutex_set_align, METH_VARARGS},
+    {"mutex_get_align", (PyCFunction)DBEnv_mutex_get_align, METH_NOARGS},
+    {"mutex_set_increment", (PyCFunction)DBEnv_mutex_set_increment,
+        METH_VARARGS},
+    {"mutex_get_increment", (PyCFunction)DBEnv_mutex_get_increment,
+        METH_NOARGS},
+    {"mutex_set_tas_spins", (PyCFunction)DBEnv_mutex_set_tas_spins,
+        METH_VARARGS},
+    {"mutex_get_tas_spins", (PyCFunction)DBEnv_mutex_get_tas_spins,
+        METH_NOARGS},
+    {"mutex_stat",      (PyCFunction)DBEnv_mutex_stat,      METH_VARARGS},
+#if (DBVER >= 44)
+    {"mutex_stat_print", (PyCFunction)DBEnv_mutex_stat_print,
+                                         METH_VARARGS|METH_KEYWORDS},
 #endif
-#if (DBVER >= 40)
-    {"set_timeout",     (PyCFunction)DBEnv_set_timeout,      METH_VARARGS|METH_KEYWORDS},
 #endif
-    {"set_shm_key",     (PyCFunction)DBEnv_set_shm_key,      METH_VARARGS},
-    {"set_cachesize",   (PyCFunction)DBEnv_set_cachesize,    METH_VARARGS},
-    {"set_data_dir",    (PyCFunction)DBEnv_set_data_dir,     METH_VARARGS},
-#if (DBVER >= 32)
-    {"set_flags",       (PyCFunction)DBEnv_set_flags,        METH_VARARGS},
-#endif
-    {"set_lg_bsize",    (PyCFunction)DBEnv_set_lg_bsize,     METH_VARARGS},
-    {"set_lg_dir",      (PyCFunction)DBEnv_set_lg_dir,       METH_VARARGS},
-    {"set_lg_max",      (PyCFunction)DBEnv_set_lg_max,       METH_VARARGS},
-#if (DBVER >= 33)
+    {"set_data_dir",    (PyCFunction)DBEnv_set_data_dir,    METH_VARARGS},
+    {"get_data_dirs",   (PyCFunction)DBEnv_get_data_dirs,   METH_NOARGS},
+    {"get_flags",       (PyCFunction)DBEnv_get_flags,       METH_NOARGS},
+    {"set_flags",       (PyCFunction)DBEnv_set_flags,       METH_VARARGS},
+#if (DBVER >= 47)
+    {"log_set_config",  (PyCFunction)DBEnv_log_set_config,  METH_VARARGS},
+    {"log_get_config",  (PyCFunction)DBEnv_log_get_config,  METH_VARARGS},
+#endif
+    {"set_lg_bsize",    (PyCFunction)DBEnv_set_lg_bsize,    METH_VARARGS},
+    {"get_lg_bsize",    (PyCFunction)DBEnv_get_lg_bsize,    METH_NOARGS},
+    {"set_lg_dir",      (PyCFunction)DBEnv_set_lg_dir,      METH_VARARGS},
+    {"get_lg_dir",      (PyCFunction)DBEnv_get_lg_dir,      METH_NOARGS},
+    {"set_lg_max",      (PyCFunction)DBEnv_set_lg_max,      METH_VARARGS},
+    {"get_lg_max",      (PyCFunction)DBEnv_get_lg_max,      METH_NOARGS},
     {"set_lg_regionmax",(PyCFunction)DBEnv_set_lg_regionmax, METH_VARARGS},
+    {"get_lg_regionmax",(PyCFunction)DBEnv_get_lg_regionmax, METH_NOARGS},
+#if (DBVER >= 44)
+    {"set_lg_filemode", (PyCFunction)DBEnv_set_lg_filemode, METH_VARARGS},
+    {"get_lg_filemode", (PyCFunction)DBEnv_get_lg_filemode, METH_NOARGS},
 #endif
-    {"set_lk_detect",   (PyCFunction)DBEnv_set_lk_detect,    METH_VARARGS},
+#if (DBVER >= 47)
+    {"set_lk_partitions", (PyCFunction)DBEnv_set_lk_partitions, METH_VARARGS},
+    {"get_lk_partitions", (PyCFunction)DBEnv_get_lk_partitions, METH_NOARGS},
+#endif
+    {"set_lk_detect",   (PyCFunction)DBEnv_set_lk_detect,   METH_VARARGS},
+    {"get_lk_detect",   (PyCFunction)DBEnv_get_lk_detect,   METH_NOARGS},
 #if (DBVER < 45)
-    {"set_lk_max",      (PyCFunction)DBEnv_set_lk_max,       METH_VARARGS},
+    {"set_lk_max",      (PyCFunction)DBEnv_set_lk_max,      METH_VARARGS},
 #endif
-#if (DBVER >= 32)
     {"set_lk_max_locks", (PyCFunction)DBEnv_set_lk_max_locks, METH_VARARGS},
+    {"get_lk_max_locks", (PyCFunction)DBEnv_get_lk_max_locks, METH_NOARGS},
     {"set_lk_max_lockers", (PyCFunction)DBEnv_set_lk_max_lockers, METH_VARARGS},
+    {"get_lk_max_lockers", (PyCFunction)DBEnv_get_lk_max_lockers, METH_NOARGS},
     {"set_lk_max_objects", (PyCFunction)DBEnv_set_lk_max_objects, METH_VARARGS},
-#endif
-    {"set_mp_mmapsize", (PyCFunction)DBEnv_set_mp_mmapsize,  METH_VARARGS},
-    {"set_tmp_dir",     (PyCFunction)DBEnv_set_tmp_dir,      METH_VARARGS},
-    {"txn_begin",       (PyCFunction)DBEnv_txn_begin,        METH_VARARGS|METH_KEYWORDS},
-    {"txn_checkpoint",  (PyCFunction)DBEnv_txn_checkpoint,   METH_VARARGS},
-    {"txn_stat",        (PyCFunction)DBEnv_txn_stat,         METH_VARARGS},
-    {"set_tx_max",      (PyCFunction)DBEnv_set_tx_max,       METH_VARARGS},
+    {"get_lk_max_objects", (PyCFunction)DBEnv_get_lk_max_objects, METH_NOARGS},
+    {"stat_print",          (PyCFunction)DBEnv_stat_print,
+        METH_VARARGS|METH_KEYWORDS},
+    {"set_mp_mmapsize", (PyCFunction)DBEnv_set_mp_mmapsize, METH_VARARGS},
+    {"get_mp_mmapsize", (PyCFunction)DBEnv_get_mp_mmapsize, METH_NOARGS},
+    {"set_tmp_dir",     (PyCFunction)DBEnv_set_tmp_dir,     METH_VARARGS},
+    {"get_tmp_dir",     (PyCFunction)DBEnv_get_tmp_dir,     METH_NOARGS},
+    {"txn_begin",       (PyCFunction)DBEnv_txn_begin,       METH_VARARGS|METH_KEYWORDS},
+    {"txn_checkpoint",  (PyCFunction)DBEnv_txn_checkpoint,  METH_VARARGS},
+    {"txn_stat",        (PyCFunction)DBEnv_txn_stat,        METH_VARARGS},
+    {"txn_stat_print",  (PyCFunction)DBEnv_txn_stat_print,
+        METH_VARARGS|METH_KEYWORDS},
+    {"get_tx_max",      (PyCFunction)DBEnv_get_tx_max,      METH_NOARGS},
+    {"get_tx_timestamp", (PyCFunction)DBEnv_get_tx_timestamp, METH_NOARGS},
+    {"set_tx_max",      (PyCFunction)DBEnv_set_tx_max,      METH_VARARGS},
     {"set_tx_timestamp", (PyCFunction)DBEnv_set_tx_timestamp, METH_VARARGS},
-    {"lock_detect",     (PyCFunction)DBEnv_lock_detect,      METH_VARARGS},
-    {"lock_get",        (PyCFunction)DBEnv_lock_get,         METH_VARARGS},
-    {"lock_id",         (PyCFunction)DBEnv_lock_id,          METH_VARARGS},
-    {"lock_put",        (PyCFunction)DBEnv_lock_put,         METH_VARARGS},
-    {"lock_stat",       (PyCFunction)DBEnv_lock_stat,        METH_VARARGS},
-    {"log_archive",     (PyCFunction)DBEnv_log_archive,      METH_VARARGS},
-#if (DBVER >= 40)
-    {"log_stat",        (PyCFunction)DBEnv_log_stat,         METH_VARARGS},
+    {"lock_detect",     (PyCFunction)DBEnv_lock_detect,     METH_VARARGS},
+    {"lock_get",        (PyCFunction)DBEnv_lock_get,        METH_VARARGS},
+    {"lock_id",         (PyCFunction)DBEnv_lock_id,         METH_NOARGS},
+    {"lock_id_free",    (PyCFunction)DBEnv_lock_id_free,    METH_VARARGS},
+    {"lock_put",        (PyCFunction)DBEnv_lock_put,        METH_VARARGS},
+    {"lock_stat",       (PyCFunction)DBEnv_lock_stat,       METH_VARARGS},
+    {"lock_stat_print", (PyCFunction)DBEnv_lock_stat_print,
+        METH_VARARGS|METH_KEYWORDS},
+    {"log_cursor",      (PyCFunction)DBEnv_log_cursor,      METH_NOARGS},
+    {"log_file",        (PyCFunction)DBEnv_log_file,        METH_VARARGS},
+#if (DBVER >= 44)
+    {"log_printf",      (PyCFunction)DBEnv_log_printf,
+        METH_VARARGS|METH_KEYWORDS},
 #endif
+    {"log_archive",     (PyCFunction)DBEnv_log_archive,     METH_VARARGS},
+    {"log_flush",       (PyCFunction)DBEnv_log_flush,       METH_NOARGS},
+    {"log_stat",        (PyCFunction)DBEnv_log_stat,        METH_VARARGS},
+    {"log_stat_print",  (PyCFunction)DBEnv_log_stat_print,
+        METH_VARARGS|METH_KEYWORDS},
 #if (DBVER >= 44)
-    {"lsn_reset",       (PyCFunction)DBEnv_lsn_reset,        METH_VARARGS|METH_KEYWORDS},
+    {"fileid_reset",    (PyCFunction)DBEnv_fileid_reset,    METH_VARARGS|METH_KEYWORDS},
+    {"lsn_reset",       (PyCFunction)DBEnv_lsn_reset,       METH_VARARGS|METH_KEYWORDS},
 #endif
     {"set_get_returns_none",(PyCFunction)DBEnv_set_get_returns_none, METH_VARARGS},
+    {"txn_recover",     (PyCFunction)DBEnv_txn_recover,     METH_NOARGS},
+#if (DBVER < 48)
+    {"set_rpc_server",  (PyCFunction)DBEnv_set_rpc_server,
+        METH_VARARGS|METH_KEYWORDS},
+#endif
+    {"set_mp_max_openfd", (PyCFunction)DBEnv_set_mp_max_openfd, METH_VARARGS},
+    {"get_mp_max_openfd", (PyCFunction)DBEnv_get_mp_max_openfd, METH_NOARGS},
+    {"set_mp_max_write", (PyCFunction)DBEnv_set_mp_max_write, METH_VARARGS},
+    {"get_mp_max_write", (PyCFunction)DBEnv_get_mp_max_write, METH_NOARGS},
+    {"set_verbose",     (PyCFunction)DBEnv_set_verbose,     METH_VARARGS},
+    {"get_verbose",     (PyCFunction)DBEnv_get_verbose,     METH_VARARGS},
+    {"set_private",     (PyCFunction)DBEnv_set_private,     METH_O},
+    {"get_private",     (PyCFunction)DBEnv_get_private,     METH_NOARGS},
+    {"get_open_flags",  (PyCFunction)DBEnv_get_open_flags,  METH_NOARGS},
+#if (DBVER >= 47)
+    {"set_intermediate_dir_mode", (PyCFunction)DBEnv_set_intermediate_dir_mode,
+        METH_VARARGS},
+    {"get_intermediate_dir_mode", (PyCFunction)DBEnv_get_intermediate_dir_mode,
+        METH_NOARGS},
+#endif
+#if (DBVER < 47)
+    {"set_intermediate_dir", (PyCFunction)DBEnv_set_intermediate_dir,
+        METH_VARARGS},
+#endif
+    {"rep_start",       (PyCFunction)DBEnv_rep_start,
+        METH_VARARGS|METH_KEYWORDS},
+    {"rep_set_transport", (PyCFunction)DBEnv_rep_set_transport, METH_VARARGS},
+    {"rep_process_message", (PyCFunction)DBEnv_rep_process_message,
+        METH_VARARGS},
+#if (DBVER >= 46)
+    {"rep_elect",       (PyCFunction)DBEnv_rep_elect,         METH_VARARGS},
+#endif
+#if (DBVER >= 44)
+    {"rep_set_config",  (PyCFunction)DBEnv_rep_set_config,    METH_VARARGS},
+    {"rep_get_config",  (PyCFunction)DBEnv_rep_get_config,    METH_VARARGS},
+    {"rep_sync",        (PyCFunction)DBEnv_rep_sync,          METH_NOARGS},
+#endif
+#if (DBVER >= 45)
+    {"rep_set_limit",   (PyCFunction)DBEnv_rep_set_limit,     METH_VARARGS},
+    {"rep_get_limit",   (PyCFunction)DBEnv_rep_get_limit,     METH_NOARGS},
+#endif
+#if (DBVER >= 47)
+    {"rep_set_request", (PyCFunction)DBEnv_rep_set_request,   METH_VARARGS},
+    {"rep_get_request", (PyCFunction)DBEnv_rep_get_request,   METH_NOARGS},
+#endif
+#if (DBVER >= 45)
+    {"set_event_notify", (PyCFunction)DBEnv_set_event_notify, METH_O},
+#endif
+#if (DBVER >= 45)
+    {"rep_set_nsites", (PyCFunction)DBEnv_rep_set_nsites, METH_VARARGS},
+    {"rep_get_nsites", (PyCFunction)DBEnv_rep_get_nsites, METH_NOARGS},
+    {"rep_set_priority", (PyCFunction)DBEnv_rep_set_priority, METH_VARARGS},
+    {"rep_get_priority", (PyCFunction)DBEnv_rep_get_priority, METH_NOARGS},
+    {"rep_set_timeout", (PyCFunction)DBEnv_rep_set_timeout, METH_VARARGS},
+    {"rep_get_timeout", (PyCFunction)DBEnv_rep_get_timeout, METH_VARARGS},
+#endif
+#if (DBVER >= 47)
+    {"rep_set_clockskew", (PyCFunction)DBEnv_rep_set_clockskew, METH_VARARGS},
+    {"rep_get_clockskew", (PyCFunction)DBEnv_rep_get_clockskew, METH_VARARGS},
+#endif
+    {"rep_stat", (PyCFunction)DBEnv_rep_stat,
+        METH_VARARGS|METH_KEYWORDS},
+    {"rep_stat_print", (PyCFunction)DBEnv_rep_stat_print,
+        METH_VARARGS|METH_KEYWORDS},
+
+#if (DBVER >= 45)
+    {"repmgr_start", (PyCFunction)DBEnv_repmgr_start,
+        METH_VARARGS|METH_KEYWORDS},
+#if (DBVER < 52)
+    {"repmgr_set_local_site", (PyCFunction)DBEnv_repmgr_set_local_site,
+        METH_VARARGS|METH_KEYWORDS},
+    {"repmgr_add_remote_site", (PyCFunction)DBEnv_repmgr_add_remote_site,
+        METH_VARARGS|METH_KEYWORDS},
+#endif
+    {"repmgr_set_ack_policy", (PyCFunction)DBEnv_repmgr_set_ack_policy,
+        METH_VARARGS},
+    {"repmgr_get_ack_policy", (PyCFunction)DBEnv_repmgr_get_ack_policy,
+        METH_NOARGS},
+    {"repmgr_site_list", (PyCFunction)DBEnv_repmgr_site_list,
+        METH_NOARGS},
+#endif
+#if (DBVER >= 46)
+    {"repmgr_stat", (PyCFunction)DBEnv_repmgr_stat,
+        METH_VARARGS|METH_KEYWORDS},
+    {"repmgr_stat_print", (PyCFunction)DBEnv_repmgr_stat_print,
+        METH_VARARGS|METH_KEYWORDS},
+#endif
+#if (DBVER >= 52)
+    {"repmgr_site", (PyCFunction)DBEnv_repmgr_site,
+        METH_VARARGS | METH_KEYWORDS},
+    {"repmgr_site_by_eid",  (PyCFunction)DBEnv_repmgr_site_by_eid,
+        METH_VARARGS | METH_KEYWORDS},
+#endif
     {NULL,      NULL}       /* sentinel */
 };
 
@@ -5295,157 +9097,249 @@
 static PyMethodDef DBTxn_methods[] = {
     {"commit",          (PyCFunction)DBTxn_commit,      METH_VARARGS},
     {"prepare",         (PyCFunction)DBTxn_prepare,     METH_VARARGS},
-    {"abort",           (PyCFunction)DBTxn_abort,       METH_VARARGS},
-    {"id",              (PyCFunction)DBTxn_id,          METH_VARARGS},
+    {"discard",         (PyCFunction)DBTxn_discard,     METH_NOARGS},
+    {"abort",           (PyCFunction)DBTxn_abort,       METH_NOARGS},
+    {"id",              (PyCFunction)DBTxn_id,          METH_NOARGS},
+    {"set_timeout",     (PyCFunction)DBTxn_set_timeout,
+        METH_VARARGS|METH_KEYWORDS},
+#if (DBVER >= 44)
+    {"set_name",        (PyCFunction)DBTxn_set_name, METH_VARARGS},
+    {"get_name",        (PyCFunction)DBTxn_get_name, METH_NOARGS},
+#endif
     {NULL,      NULL}       /* sentinel */
 };
 
 
-#if (DBVER >= 43)
 static PyMethodDef DBSequence_methods[] = {
     {"close",           (PyCFunction)DBSequence_close,          METH_VARARGS},
     {"get",             (PyCFunction)DBSequence_get,            METH_VARARGS|METH_KEYWORDS},
-    {"get_dbp",         (PyCFunction)DBSequence_get_dbp,        METH_VARARGS},
-    {"get_key",         (PyCFunction)DBSequence_get_key,        METH_VARARGS},
-    {"init_value",      (PyCFunction)DBSequence_init_value,     METH_VARARGS},
+    {"get_dbp",         (PyCFunction)DBSequence_get_dbp,        METH_NOARGS},
+    {"get_key",         (PyCFunction)DBSequence_get_key,        METH_NOARGS},
+    {"initial_value",   (PyCFunction)DBSequence_initial_value,  METH_VARARGS},
     {"open",            (PyCFunction)DBSequence_open,           METH_VARARGS|METH_KEYWORDS},
     {"remove",          (PyCFunction)DBSequence_remove,         METH_VARARGS|METH_KEYWORDS},
     {"set_cachesize",   (PyCFunction)DBSequence_set_cachesize,  METH_VARARGS},
-    {"get_cachesize",   (PyCFunction)DBSequence_get_cachesize,  METH_VARARGS},
+    {"get_cachesize",   (PyCFunction)DBSequence_get_cachesize,  METH_NOARGS},
     {"set_flags",       (PyCFunction)DBSequence_set_flags,      METH_VARARGS},
-    {"get_flags",       (PyCFunction)DBSequence_get_flags,      METH_VARARGS},
+    {"get_flags",       (PyCFunction)DBSequence_get_flags,      METH_NOARGS},
     {"set_range",       (PyCFunction)DBSequence_set_range,      METH_VARARGS},
-    {"get_range",       (PyCFunction)DBSequence_get_range,      METH_VARARGS},
+    {"get_range",       (PyCFunction)DBSequence_get_range,      METH_NOARGS},
     {"stat",            (PyCFunction)DBSequence_stat,           METH_VARARGS|METH_KEYWORDS},
+    {"stat_print",      (PyCFunction)DBSequence_stat_print,
+        METH_VARARGS|METH_KEYWORDS},
     {NULL,      NULL}       /* sentinel */
 };
-#endif
 
 
 static PyObject*
-DB_getattr(DBObject* self, char *name)
+DBEnv_db_home_get(DBEnvObject* self)
 {
-    return Py_FindMethod(DB_methods, (PyObject* )self, name);
-}
+    const char *home = NULL;
 
+    CHECK_ENV_NOT_CLOSED(self);
 
-static PyObject*
-DBEnv_getattr(DBEnvObject* self, char *name)
-{
-    if (!strcmp(name, "db_home")) {
-        CHECK_ENV_NOT_CLOSED(self);
-        if (self->db_env->db_home == NULL) {
-            RETURN_NONE();
-        }
-        return PyString_FromString(self->db_env->db_home);
-    }
+    MYDB_BEGIN_ALLOW_THREADS;
+    self->db_env->get_home(self->db_env, &home);
+    MYDB_END_ALLOW_THREADS;
 
-    return Py_FindMethod(DBEnv_methods, (PyObject* )self, name);
+    if (home == NULL) {
+        RETURN_NONE();
+    }
+    return PyBytes_FromString(home);
 }
 
+static PyGetSetDef DBEnv_getsets[] = {
+    {"db_home", (getter)DBEnv_db_home_get, NULL,},
+    {NULL}
+};
 
-static PyObject*
-DBCursor_getattr(DBCursorObject* self, char *name)
-{
-    return Py_FindMethod(DBCursor_methods, (PyObject* )self, name);
-}
 
-static PyObject*
-DBTxn_getattr(DBTxnObject* self, char *name)
-{
-    return Py_FindMethod(DBTxn_methods, (PyObject* )self, name);
-}
+statichere PyTypeObject DB_Type = {
+#if (PY_VERSION_HEX < 0x03000000)
+    PyObject_HEAD_INIT(NULL)
+    0,                  /*ob_size*/
+#else
+    PyVarObject_HEAD_INIT(NULL, 0)
+#endif
+    "DB",               /*tp_name*/
+    sizeof(DBObject),   /*tp_basicsize*/
+    0,                  /*tp_itemsize*/
+    /* methods */
+    (destructor)DB_dealloc, /*tp_dealloc*/
+    0,          /*tp_print*/
+    0,          /*tp_getattr*/
+    0,          /*tp_setattr*/
+    0,          /*tp_compare*/
+    0,          /*tp_repr*/
+    0,          /*tp_as_number*/
+    &DB_sequence,/*tp_as_sequence*/
+    &DB_mapping,/*tp_as_mapping*/
+    0,          /*tp_hash*/
+    0,                  /* tp_call */
+    0,                  /* tp_str */
+    0,                  /* tp_getattro */
+    0,          /* tp_setattro */
+    0,                  /* tp_as_buffer */
+#if (PY_VERSION_HEX < 0x03000000)
+    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_WEAKREFS,      /* tp_flags */
+#else
+    Py_TPFLAGS_DEFAULT,      /* tp_flags */
+#endif
+    0,          /* tp_doc */
+    0,              /* tp_traverse */
+    0,                  /* tp_clear */
+    0,                  /* tp_richcompare */
+    offsetof(DBObject, in_weakreflist),   /* tp_weaklistoffset */
+    0,          /*tp_iter*/
+    0,          /*tp_iternext*/
+    DB_methods, /*tp_methods*/
+    0, /*tp_members*/
+};
 
-static PyObject*
-DBLock_getattr(DBLockObject* self, char *name)
-{
-    return NULL;
-}
 
-#if (DBVER >= 43)
-static PyObject*
-DBSequence_getattr(DBSequenceObject* self, char *name)
-{
-    return Py_FindMethod(DBSequence_methods, (PyObject* )self, name);
-}
+statichere PyTypeObject DBCursor_Type = {
+#if (PY_VERSION_HEX < 0x03000000)
+    PyObject_HEAD_INIT(NULL)
+    0,                  /*ob_size*/
+#else
+    PyVarObject_HEAD_INIT(NULL, 0)
+#endif
+    "DBCursor",         /*tp_name*/
+    sizeof(DBCursorObject),  /*tp_basicsize*/
+    0,          /*tp_itemsize*/
+    /* methods */
+    (destructor)DBCursor_dealloc,/*tp_dealloc*/
+    0,          /*tp_print*/
+    0,          /*tp_getattr*/
+    0,          /*tp_setattr*/
+    0,          /*tp_compare*/
+    0,          /*tp_repr*/
+    0,          /*tp_as_number*/
+    0,          /*tp_as_sequence*/
+    0,          /*tp_as_mapping*/
+    0,          /*tp_hash*/
+    0,          /*tp_call*/
+    0,          /*tp_str*/
+    0,          /*tp_getattro*/
+    0,          /*tp_setattro*/
+    0,          /*tp_as_buffer*/
+#if (PY_VERSION_HEX < 0x03000000)
+    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_WEAKREFS,      /* tp_flags */
+#else
+    Py_TPFLAGS_DEFAULT,      /* tp_flags */
 #endif
+    0,          /* tp_doc */
+    0,          /* tp_traverse */
+    0,          /* tp_clear */
+    0,          /* tp_richcompare */
+    offsetof(DBCursorObject, in_weakreflist),   /* tp_weaklistoffset */
+    0,          /*tp_iter*/
+    0,          /*tp_iternext*/
+    DBCursor_methods, /*tp_methods*/
+    0,          /*tp_members*/
+};
 
-statichere PyTypeObject DB_Type = {
+
+statichere PyTypeObject DBLogCursor_Type = {
+#if (PY_VERSION_HEX < 0x03000000)
     PyObject_HEAD_INIT(NULL)
     0,                  /*ob_size*/
-    "DB",               /*tp_name*/
-    sizeof(DBObject),   /*tp_basicsize*/
-    0,                  /*tp_itemsize*/
+#else
+    PyVarObject_HEAD_INIT(NULL, 0)
+#endif
+    "DBLogCursor",         /*tp_name*/
+    sizeof(DBLogCursorObject),  /*tp_basicsize*/
+    0,          /*tp_itemsize*/
     /* methods */
-    (destructor)DB_dealloc, /*tp_dealloc*/
-    0,                  /*tp_print*/
-    (getattrfunc)DB_getattr, /*tp_getattr*/
-    0,                      /*tp_setattr*/
+    (destructor)DBLogCursor_dealloc,/*tp_dealloc*/
+    0,          /*tp_print*/
+    0,          /*tp_getattr*/
+    0,          /*tp_setattr*/
     0,          /*tp_compare*/
     0,          /*tp_repr*/
     0,          /*tp_as_number*/
     0,          /*tp_as_sequence*/
-    &DB_mapping,/*tp_as_mapping*/
+    0,          /*tp_as_mapping*/
     0,          /*tp_hash*/
-#ifdef HAVE_WEAKREF
-    0,			/* tp_call */
-    0,			/* tp_str */
-    0,  		/* tp_getattro */
-    0,                  /* tp_setattro */
-    0,			/* tp_as_buffer */
+    0,          /*tp_call*/
+    0,          /*tp_str*/
+    0,          /*tp_getattro*/
+    0,          /*tp_setattro*/
+    0,          /*tp_as_buffer*/
+#if (PY_VERSION_HEX < 0x03000000)
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_WEAKREFS,      /* tp_flags */
-    0,                  /* tp_doc */
-    0,		        /* tp_traverse */
-    0,			/* tp_clear */
-    0,			/* tp_richcompare */
-    offsetof(DBObject, in_weakreflist),   /* tp_weaklistoffset */
+#else
+    Py_TPFLAGS_DEFAULT,      /* tp_flags */
 #endif
+    0,          /* tp_doc */
+    0,          /* tp_traverse */
+    0,          /* tp_clear */
+    0,          /* tp_richcompare */
+    offsetof(DBLogCursorObject, in_weakreflist),   /* tp_weaklistoffset */
+    0,          /*tp_iter*/
+    0,          /*tp_iternext*/
+    DBLogCursor_methods, /*tp_methods*/
+    0,          /*tp_members*/
 };
 
-
-statichere PyTypeObject DBCursor_Type = {
+#if (DBVER >= 52)
+statichere PyTypeObject DBSite_Type = {
+#if (PY_VERSION_HEX < 0x03000000)
     PyObject_HEAD_INIT(NULL)
     0,                  /*ob_size*/
-    "DBCursor",         /*tp_name*/
-    sizeof(DBCursorObject),  /*tp_basicsize*/
-    0,                  /*tp_itemsize*/
+#else
+    PyVarObject_HEAD_INIT(NULL, 0)
+#endif
+    "DBSite",         /*tp_name*/
+    sizeof(DBSiteObject),  /*tp_basicsize*/
+    0,          /*tp_itemsize*/
     /* methods */
-    (destructor)DBCursor_dealloc,/*tp_dealloc*/
-    0,                  /*tp_print*/
-    (getattrfunc)DBCursor_getattr, /*tp_getattr*/
-    0,                  /*tp_setattr*/
-    0,                  /*tp_compare*/
-    0,                  /*tp_repr*/
-    0,                  /*tp_as_number*/
-    0,                  /*tp_as_sequence*/
-    0,                  /*tp_as_mapping*/
-    0,                  /*tp_hash*/
-#ifdef HAVE_WEAKREF
-    0,			/* tp_call */
-    0,			/* tp_str */
-    0,  		/* tp_getattro */
-    0,                  /* tp_setattro */
-    0,			/* tp_as_buffer */
+    (destructor)DBSite_dealloc,/*tp_dealloc*/
+    0,          /*tp_print*/
+    0,          /*tp_getattr*/
+    0,          /*tp_setattr*/
+    0,          /*tp_compare*/
+    0,          /*tp_repr*/
+    0,          /*tp_as_number*/
+    0,          /*tp_as_sequence*/
+    0,          /*tp_as_mapping*/
+    0,          /*tp_hash*/
+    0,          /*tp_call*/
+    0,          /*tp_str*/
+    0,          /*tp_getattro*/
+    0,          /*tp_setattro*/
+    0,          /*tp_as_buffer*/
+#if (PY_VERSION_HEX < 0x03000000)
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_WEAKREFS,      /* tp_flags */
-    0,                  /* tp_doc */
-    0,		        /* tp_traverse */
-    0,			/* tp_clear */
-    0,			/* tp_richcompare */
-    offsetof(DBCursorObject, in_weakreflist),   /* tp_weaklistoffset */
+#else
+    Py_TPFLAGS_DEFAULT,      /* tp_flags */
 #endif
+    0,          /* tp_doc */
+    0,          /* tp_traverse */
+    0,          /* tp_clear */
+    0,          /* tp_richcompare */
+    offsetof(DBSiteObject, in_weakreflist),   /* tp_weaklistoffset */
+    0,          /*tp_iter*/
+    0,          /*tp_iternext*/
+    DBSite_methods, /*tp_methods*/
+    0,          /*tp_members*/
 };
-
+#endif
 
 statichere PyTypeObject DBEnv_Type = {
+#if (PY_VERSION_HEX < 0x03000000)
     PyObject_HEAD_INIT(NULL)
-    0,          /*ob_size*/
+    0,                  /*ob_size*/
+#else
+    PyVarObject_HEAD_INIT(NULL, 0)
+#endif
     "DBEnv",            /*tp_name*/
     sizeof(DBEnvObject),    /*tp_basicsize*/
     0,          /*tp_itemsize*/
     /* methods */
     (destructor)DBEnv_dealloc, /*tp_dealloc*/
     0,          /*tp_print*/
-    (getattrfunc)DBEnv_getattr, /*tp_getattr*/
+    0,          /*tp_getattr*/
     0,          /*tp_setattr*/
     0,          /*tp_compare*/
     0,          /*tp_repr*/
@@ -5453,97 +9347,123 @@
     0,          /*tp_as_sequence*/
     0,          /*tp_as_mapping*/
     0,          /*tp_hash*/
-#ifdef HAVE_WEAKREF
-    0,			/* tp_call */
-    0,			/* tp_str */
-    0,  		/* tp_getattro */
-    0,                  /* tp_setattro */
-    0,			/* tp_as_buffer */
+    0,                  /* tp_call */
+    0,                  /* tp_str */
+    0,                  /* tp_getattro */
+    0,          /* tp_setattro */
+    0,                  /* tp_as_buffer */
+#if (PY_VERSION_HEX < 0x03000000)
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_WEAKREFS,      /* tp_flags */
-    0,                  /* tp_doc */
-    0,		        /* tp_traverse */
-    0,			/* tp_clear */
-    0,			/* tp_richcompare */
-    offsetof(DBEnvObject, in_weakreflist),   /* tp_weaklistoffset */
+#else
+    Py_TPFLAGS_DEFAULT,      /* tp_flags */
 #endif
+    0,          /* tp_doc */
+    0,              /* tp_traverse */
+    0,                  /* tp_clear */
+    0,                  /* tp_richcompare */
+    offsetof(DBEnvObject, in_weakreflist),   /* tp_weaklistoffset */
+    0,          /* tp_iter */
+    0,          /* tp_iternext */
+    DBEnv_methods,      /* tp_methods */
+    0,          /* tp_members */
+    DBEnv_getsets,      /* tp_getsets */
 };
 
 statichere PyTypeObject DBTxn_Type = {
+#if (PY_VERSION_HEX < 0x03000000)
     PyObject_HEAD_INIT(NULL)
-    0,          /*ob_size*/
+    0,                  /*ob_size*/
+#else
+    PyVarObject_HEAD_INIT(NULL, 0)
+#endif
     "DBTxn",    /*tp_name*/
     sizeof(DBTxnObject),  /*tp_basicsize*/
     0,          /*tp_itemsize*/
     /* methods */
     (destructor)DBTxn_dealloc, /*tp_dealloc*/
     0,          /*tp_print*/
-    (getattrfunc)DBTxn_getattr, /*tp_getattr*/
-    0,                      /*tp_setattr*/
+    0,          /*tp_getattr*/
+    0,          /*tp_setattr*/
     0,          /*tp_compare*/
     0,          /*tp_repr*/
     0,          /*tp_as_number*/
     0,          /*tp_as_sequence*/
     0,          /*tp_as_mapping*/
     0,          /*tp_hash*/
-#ifdef HAVE_WEAKREF
-    0,			/* tp_call */
-    0,			/* tp_str */
-    0,  		/* tp_getattro */
-    0,                  /* tp_setattro */
-    0,			/* tp_as_buffer */
+    0,                  /* tp_call */
+    0,                  /* tp_str */
+    0,                  /* tp_getattro */
+    0,          /* tp_setattro */
+    0,                  /* tp_as_buffer */
+#if (PY_VERSION_HEX < 0x03000000)
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_WEAKREFS,      /* tp_flags */
-    0,                  /* tp_doc */
-    0,		        /* tp_traverse */
-    0,			/* tp_clear */
-    0,			/* tp_richcompare */
-    offsetof(DBTxnObject, in_weakreflist),   /* tp_weaklistoffset */
+#else
+    Py_TPFLAGS_DEFAULT,      /* tp_flags */
 #endif
+    0,          /* tp_doc */
+    0,          /* tp_traverse */
+    0,                  /* tp_clear */
+    0,                  /* tp_richcompare */
+    offsetof(DBTxnObject, in_weakreflist),   /* tp_weaklistoffset */
+    0,          /*tp_iter*/
+    0,          /*tp_iternext*/
+    DBTxn_methods, /*tp_methods*/
+    0,          /*tp_members*/
 };
 
 
 statichere PyTypeObject DBLock_Type = {
+#if (PY_VERSION_HEX < 0x03000000)
     PyObject_HEAD_INIT(NULL)
-    0,          /*ob_size*/
+    0,                  /*ob_size*/
+#else
+    PyVarObject_HEAD_INIT(NULL, 0)
+#endif
     "DBLock",   /*tp_name*/
     sizeof(DBLockObject),  /*tp_basicsize*/
     0,          /*tp_itemsize*/
     /* methods */
     (destructor)DBLock_dealloc, /*tp_dealloc*/
     0,          /*tp_print*/
-    (getattrfunc)DBLock_getattr, /*tp_getattr*/
-    0,                      /*tp_setattr*/
+    0,          /*tp_getattr*/
+    0,          /*tp_setattr*/
     0,          /*tp_compare*/
     0,          /*tp_repr*/
     0,          /*tp_as_number*/
     0,          /*tp_as_sequence*/
     0,          /*tp_as_mapping*/
     0,          /*tp_hash*/
-#ifdef HAVE_WEAKREF
-    0,			/* tp_call */
-    0,			/* tp_str */
-    0,  		/* tp_getattro */
-    0,                  /* tp_setattro */
-    0,			/* tp_as_buffer */
+    0,                  /* tp_call */
+    0,                  /* tp_str */
+    0,                  /* tp_getattro */
+    0,          /* tp_setattro */
+    0,                  /* tp_as_buffer */
+#if (PY_VERSION_HEX < 0x03000000)
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_WEAKREFS,      /* tp_flags */
-    0,                  /* tp_doc */
-    0,		        /* tp_traverse */
-    0,			/* tp_clear */
-    0,			/* tp_richcompare */
-    offsetof(DBLockObject, in_weakreflist),   /* tp_weaklistoffset */
+#else
+    Py_TPFLAGS_DEFAULT,      /* tp_flags */
 #endif
+    0,          /* tp_doc */
+    0,              /* tp_traverse */
+    0,                  /* tp_clear */
+    0,                  /* tp_richcompare */
+    offsetof(DBLockObject, in_weakreflist),   /* tp_weaklistoffset */
 };
 
-#if (DBVER >= 43)
 statichere PyTypeObject DBSequence_Type = {
+#if (PY_VERSION_HEX < 0x03000000)
     PyObject_HEAD_INIT(NULL)
-    0,          /*ob_size*/
+    0,                  /*ob_size*/
+#else
+    PyVarObject_HEAD_INIT(NULL, 0)
+#endif
     "DBSequence",                   /*tp_name*/
     sizeof(DBSequenceObject),       /*tp_basicsize*/
     0,          /*tp_itemsize*/
     /* methods */
     (destructor)DBSequence_dealloc, /*tp_dealloc*/
     0,          /*tp_print*/
-    (getattrfunc)DBSequence_getattr,/*tp_getattr*/
+    0,          /*tp_getattr*/
     0,          /*tp_setattr*/
     0,          /*tp_compare*/
     0,          /*tp_repr*/
@@ -5551,21 +9471,26 @@
     0,          /*tp_as_sequence*/
     0,          /*tp_as_mapping*/
     0,          /*tp_hash*/
-#ifdef HAVE_WEAKREF
-    0,			/* tp_call */
-    0,			/* tp_str */
-    0,  		/* tp_getattro */
+    0,                  /* tp_call */
+    0,                  /* tp_str */
+    0,                  /* tp_getattro */
     0,          /* tp_setattro */
-    0,			/* tp_as_buffer */
+    0,                  /* tp_as_buffer */
+#if (PY_VERSION_HEX < 0x03000000)
     Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_WEAKREFS,      /* tp_flags */
+#else
+    Py_TPFLAGS_DEFAULT,      /* tp_flags */
+#endif
     0,          /* tp_doc */
-    0,		    /* tp_traverse */
-    0,			/* tp_clear */
-    0,			/* tp_richcompare */
+    0,              /* tp_traverse */
+    0,                  /* tp_clear */
+    0,                  /* tp_richcompare */
     offsetof(DBSequenceObject, in_weakreflist),   /* tp_weaklistoffset */
-#endif
+    0,          /*tp_iter*/
+    0,          /*tp_iternext*/
+    DBSequence_methods, /*tp_methods*/
+    0,          /*tp_members*/
 };
-#endif
 
 /* --------------------------------------------------------------------- */
 /* Module-level functions */
@@ -5599,7 +9524,6 @@
     return (PyObject* )newDBEnvObject(flags);
 }
 
-#if (DBVER >= 43)
 static PyObject*
 DBSequence_construct(PyObject* self, PyObject* args, PyObject* kwargs)
 {
@@ -5615,37 +9539,53 @@
     }
     return (PyObject* )newDBSequenceObject((DBObject*)dbobj, flags);
 }
-#endif
 
 static char bsddb_version_doc[] =
 "Returns a tuple of major, minor, and patch release numbers of the\n\
 underlying DB library.";
 
 static PyObject*
-bsddb_version(PyObject* self, PyObject* args)
+bsddb_version(PyObject* self)
 {
     int major, minor, patch;
 
-        if (!PyArg_ParseTuple(args, ":version"))
-        return NULL;
-        db_version(&major, &minor, &patch);
-        return Py_BuildValue("(iii)", major, minor, patch);
+    /* This should be instantaneous, no need to release the GIL */
+    db_version(&major, &minor, &patch);
+    return Py_BuildValue("(iii)", major, minor, patch);
 }
 
+#if (DBVER >= 50)
+static PyObject*
+bsddb_version_full(PyObject* self)
+{
+    char *version_string;
+    int family, release, major, minor, patch;
+
+    /* This should be instantaneous, no need to release the GIL */
+    version_string = db_full_version(&family, &release, &major, &minor, &patch);
+    return Py_BuildValue("(siiiii)",
+            version_string, family, release, major, minor, patch);
+}
+#endif
 
-/* List of functions defined in the module */
 
+/* List of functions defined in the module */
 static PyMethodDef bsddb_methods[] = {
     {"DB",          (PyCFunction)DB_construct,          METH_VARARGS | METH_KEYWORDS },
     {"DBEnv",       (PyCFunction)DBEnv_construct,       METH_VARARGS},
-#if (DBVER >= 43)    
     {"DBSequence",  (PyCFunction)DBSequence_construct,  METH_VARARGS | METH_KEYWORDS },
-#endif    
-    {"version",     (PyCFunction)bsddb_version,         METH_VARARGS, bsddb_version_doc},
+    {"version",     (PyCFunction)bsddb_version,         METH_NOARGS, bsddb_version_doc},
+#if (DBVER >= 50)
+    {"full_version", (PyCFunction)bsddb_version_full, METH_NOARGS},
+#endif
     {NULL,      NULL}       /* sentinel */
 };
 
 
+/* API structure */
+static BSDDB_api bsddb_api;
+
+
 /* --------------------------------------------------------------------- */
 /* Module initialization */
 
@@ -5655,38 +9595,86 @@
  */
 #define ADD_INT(dict, NAME)         _addIntToDict(dict, #NAME, NAME)
 
+/*
+** We can rename the module at import time, so the string allocated
+** must be big enough, and any use of the name must use this particular
+** string.
+*/
 #define MODULE_NAME_MAX_LEN     11
 static char _bsddbModuleName[MODULE_NAME_MAX_LEN+1] = "_bsddb";
 
+#if (PY_VERSION_HEX >= 0x03000000)
+static struct PyModuleDef bsddbmodule = {
+    PyModuleDef_HEAD_INIT,
+    _bsddbModuleName,   /* Name of module */
+    NULL,               /* module documentation, may be NULL */
+    -1,                 /* size of per-interpreter state of the module,
+                            or -1 if the module keeps state in global variables. */
+    bsddb_methods,
+    NULL,   /* Reload */
+    NULL,   /* Traverse */
+    NULL,   /* Clear */
+    NULL    /* Free */
+};
+#endif
+
+
+#if (PY_VERSION_HEX < 0x03000000)
 DL_EXPORT(void) init_bsddb(void)
+#else
+PyMODINIT_FUNC  PyInit__bsddb(void)    /* Note the two underscores */
+#endif
 {
     PyObject* m;
     PyObject* d;
-    PyObject* pybsddb_version_s = PyString_FromString( PY_BSDDB_VERSION );
-    PyObject* db_version_s = PyString_FromString( DB_VERSION_STRING );
-    PyObject* cvsid_s = PyString_FromString( rcs_id );
-
-    /* Initialize the type of the new type objects here; doing it here
-       is required for portability to Windows without requiring C++. */
-    DB_Type.ob_type = &PyType_Type;
-    DBCursor_Type.ob_type = &PyType_Type;
-    DBEnv_Type.ob_type = &PyType_Type;
-    DBTxn_Type.ob_type = &PyType_Type;
-    DBLock_Type.ob_type = &PyType_Type;
-#if (DBVER >= 43)    
-    DBSequence_Type.ob_type = &PyType_Type;
-#endif    
-
-
-#if defined(WITH_THREAD) && !defined(MYDB_USE_GILSTATE)
-    /* Save the current interpreter, so callbacks can do the right thing. */
-    _db_interpreterState = PyThreadState_GET()->interp;
+    PyObject* py_api;
+    PyObject* pybsddb_version_s;
+    PyObject* db_version_s;
+    PyObject* cvsid_s;
+
+#if (PY_VERSION_HEX < 0x03000000)
+    pybsddb_version_s = PyString_FromString(PY_BSDDB_VERSION);
+    db_version_s = PyString_FromString(DB_VERSION_STRING);
+    cvsid_s = PyString_FromString(rcs_id);
+#else
+    /* This data should be ascii, so UTF-8 conversion is fine */
+    pybsddb_version_s = PyUnicode_FromString(PY_BSDDB_VERSION);
+    db_version_s = PyUnicode_FromString(DB_VERSION_STRING);
+    cvsid_s = PyUnicode_FromString(rcs_id);
+#endif
+
+    /* Initialize object types */
+    if ((PyType_Ready(&DB_Type) < 0)
+        || (PyType_Ready(&DBCursor_Type) < 0)
+        || (PyType_Ready(&DBLogCursor_Type) < 0)
+        || (PyType_Ready(&DBEnv_Type) < 0)
+        || (PyType_Ready(&DBTxn_Type) < 0)
+        || (PyType_Ready(&DBLock_Type) < 0)
+        || (PyType_Ready(&DBSequence_Type) < 0)
+#if (DBVER >= 52)
+        || (PyType_Ready(&DBSite_Type) < 0)
+#endif
+        ) {
+#if (PY_VERSION_HEX < 0x03000000)
+        return;
+#else
+        return NULL;
 #endif
+    }
 
     /* Create the module and add the functions */
+#if (PY_VERSION_HEX < 0x03000000)
     m = Py_InitModule(_bsddbModuleName, bsddb_methods);
-    if (m == NULL)
-    	return;
+#else
+    m=PyModule_Create(&bsddbmodule);
+#endif
+    if (m == NULL) {
+#if (PY_VERSION_HEX < 0x03000000)
+        return;
+#else
+        return NULL;
+#endif
+    }
 
     /* Add some symbolic constants to the module */
     d = PyModule_GetDict(m);
@@ -5707,18 +9695,20 @@
     ADD_INT(d, DB_MAX_PAGES);
     ADD_INT(d, DB_MAX_RECORDS);
 
-#if (DBVER >= 42)
+#if (DBVER < 48)
     ADD_INT(d, DB_RPCCLIENT);
-#else
-    ADD_INT(d, DB_CLIENT);
-    /* allow apps to be written using DB_RPCCLIENT on older BerkeleyDB */
-    _addIntToDict(d, "DB_RPCCLIENT", DB_CLIENT);
 #endif
+
+#if (DBVER < 48)
     ADD_INT(d, DB_XA_CREATE);
+#endif
 
     ADD_INT(d, DB_CREATE);
     ADD_INT(d, DB_NOMMAP);
     ADD_INT(d, DB_THREAD);
+#if (DBVER >= 45)
+    ADD_INT(d, DB_MULTIVERSION);
+#endif
 
     ADD_INT(d, DB_FORCE);
     ADD_INT(d, DB_INIT_CDB);
@@ -5726,8 +9716,14 @@
     ADD_INT(d, DB_INIT_LOG);
     ADD_INT(d, DB_INIT_MPOOL);
     ADD_INT(d, DB_INIT_TXN);
-#if (DBVER >= 32)
     ADD_INT(d, DB_JOINENV);
+
+#if (DBVER >= 48)
+    ADD_INT(d, DB_GID_SIZE);
+#else
+    ADD_INT(d, DB_XIDDATASIZE);
+    /* Allow new code to work in old BDB releases */
+    _addIntToDict(d, "DB_GID_SIZE", DB_XIDDATASIZE);
 #endif
 
     ADD_INT(d, DB_RECOVER);
@@ -5743,27 +9739,35 @@
     ADD_INT(d, DB_TXN_SYNC);
     ADD_INT(d, DB_TXN_NOWAIT);
 
+#if (DBVER >= 51)
+    ADD_INT(d, DB_TXN_BULK);
+#endif
+
+#if (DBVER >= 48)
+    ADD_INT(d, DB_CURSOR_BULK);
+#endif
+
+#if (DBVER >= 46)
+    ADD_INT(d, DB_TXN_WAIT);
+#endif
+
     ADD_INT(d, DB_EXCL);
     ADD_INT(d, DB_FCNTL_LOCKING);
     ADD_INT(d, DB_ODDFILESIZE);
     ADD_INT(d, DB_RDWRMASTER);
     ADD_INT(d, DB_RDONLY);
     ADD_INT(d, DB_TRUNCATE);
-#if (DBVER >= 32)
     ADD_INT(d, DB_EXTENT);
     ADD_INT(d, DB_CDB_ALLDB);
     ADD_INT(d, DB_VERIFY);
-#endif
     ADD_INT(d, DB_UPGRADE);
 
+    ADD_INT(d, DB_PRINTABLE);
     ADD_INT(d, DB_AGGRESSIVE);
     ADD_INT(d, DB_NOORDERCHK);
     ADD_INT(d, DB_ORDERCHKONLY);
     ADD_INT(d, DB_PR_PAGE);
-#if ! (DBVER >= 33)
-    ADD_INT(d, DB_VRFY_FLAGMASK);
-    ADD_INT(d, DB_PR_HEADERS);
-#endif
+
     ADD_INT(d, DB_PR_RECOVERYTEST);
     ADD_INT(d, DB_SALVAGE);
 
@@ -5772,19 +9776,14 @@
     ADD_INT(d, DB_LOCK_OLDEST);
     ADD_INT(d, DB_LOCK_RANDOM);
     ADD_INT(d, DB_LOCK_YOUNGEST);
-#if (DBVER >= 33)
     ADD_INT(d, DB_LOCK_MAXLOCKS);
     ADD_INT(d, DB_LOCK_MINLOCKS);
     ADD_INT(d, DB_LOCK_MINWRITE);
-#endif
 
+    ADD_INT(d, DB_LOCK_EXPIRE);
+    ADD_INT(d, DB_LOCK_MAXWRITE);
 
-#if (DBVER >= 33)
-    /* docs say to use zero instead */
     _addIntToDict(d, "DB_LOCK_CONFLICT", 0);
-#else
-    ADD_INT(d, DB_LOCK_CONFLICT);
-#endif
 
     ADD_INT(d, DB_LOCK_DUMP);
     ADD_INT(d, DB_LOCK_GET);
@@ -5797,54 +9796,37 @@
     ADD_INT(d, DB_LOCK_READ);
     ADD_INT(d, DB_LOCK_WRITE);
     ADD_INT(d, DB_LOCK_NOWAIT);
-#if (DBVER >= 32)
     ADD_INT(d, DB_LOCK_WAIT);
-#endif
     ADD_INT(d, DB_LOCK_IWRITE);
     ADD_INT(d, DB_LOCK_IREAD);
     ADD_INT(d, DB_LOCK_IWR);
-#if (DBVER >= 33)
 #if (DBVER < 44)
     ADD_INT(d, DB_LOCK_DIRTY);
 #else
     ADD_INT(d, DB_LOCK_READ_UNCOMMITTED);  /* renamed in 4.4 */
 #endif
     ADD_INT(d, DB_LOCK_WWRITE);
-#endif
 
     ADD_INT(d, DB_LOCK_RECORD);
     ADD_INT(d, DB_LOCK_UPGRADE);
-#if (DBVER >= 32)
     ADD_INT(d, DB_LOCK_SWITCH);
-#endif
-#if (DBVER >= 33)
     ADD_INT(d, DB_LOCK_UPGRADE_WRITE);
-#endif
 
     ADD_INT(d, DB_LOCK_NOWAIT);
     ADD_INT(d, DB_LOCK_RECORD);
     ADD_INT(d, DB_LOCK_UPGRADE);
 
-#if (DBVER >= 33)
     ADD_INT(d, DB_LSTAT_ABORTED);
-#if (DBVER < 43)
-    ADD_INT(d, DB_LSTAT_ERR);
-#endif
     ADD_INT(d, DB_LSTAT_FREE);
     ADD_INT(d, DB_LSTAT_HELD);
-#if (DBVER == 33)
-    ADD_INT(d, DB_LSTAT_NOGRANT);
-#endif
+
     ADD_INT(d, DB_LSTAT_PENDING);
     ADD_INT(d, DB_LSTAT_WAITING);
-#endif
 
     ADD_INT(d, DB_ARCH_ABS);
     ADD_INT(d, DB_ARCH_DATA);
     ADD_INT(d, DB_ARCH_LOG);
-#if (DBVER >= 42)
     ADD_INT(d, DB_ARCH_REMOVE);
-#endif
 
     ADD_INT(d, DB_BTREE);
     ADD_INT(d, DB_HASH);
@@ -5859,6 +9841,8 @@
     ADD_INT(d, DB_REVSPLITOFF);
     ADD_INT(d, DB_SNAPSHOT);
 
+    ADD_INT(d, DB_INORDER);
+
     ADD_INT(d, DB_JOIN_NOSORT);
 
     ADD_INT(d, DB_AFTER);
@@ -5867,26 +9851,15 @@
 #if (DBVER < 45)
     ADD_INT(d, DB_CACHED_COUNTS);
 #endif
-#if (DBVER >= 41)
-    _addIntToDict(d, "DB_CHECKPOINT", 0);
-#else
-    ADD_INT(d, DB_CHECKPOINT);
-    ADD_INT(d, DB_CURLSN);
-#endif
-#if ((DBVER >= 33) && (DBVER <= 41))
-    ADD_INT(d, DB_COMMIT);
-#endif
+
     ADD_INT(d, DB_CONSUME);
-#if (DBVER >= 32)
     ADD_INT(d, DB_CONSUME_WAIT);
-#endif
     ADD_INT(d, DB_CURRENT);
-#if (DBVER >= 33)
     ADD_INT(d, DB_FAST_STAT);
-#endif
     ADD_INT(d, DB_FIRST);
     ADD_INT(d, DB_FLUSH);
     ADD_INT(d, DB_GET_BOTH);
+    ADD_INT(d, DB_GET_BOTH_RANGE);
     ADD_INT(d, DB_GET_RECNO);
     ADD_INT(d, DB_JOIN_ITEM);
     ADD_INT(d, DB_KEYFIRST);
@@ -5901,6 +9874,9 @@
     ADD_INT(d, DB_POSITION);
     ADD_INT(d, DB_PREV);
     ADD_INT(d, DB_PREV_NODUP);
+#if (DBVER >= 46)
+    ADD_INT(d, DB_PREV_DUP);
+#endif
 #if (DBVER < 45)
     ADD_INT(d, DB_RECORDCOUNT);
 #endif
@@ -5911,76 +9887,289 @@
 
     ADD_INT(d, DB_OPFLAGS_MASK);
     ADD_INT(d, DB_RMW);
-#if (DBVER >= 33)
     ADD_INT(d, DB_DIRTY_READ);
     ADD_INT(d, DB_MULTIPLE);
     ADD_INT(d, DB_MULTIPLE_KEY);
-#endif
 
 #if (DBVER >= 44)
+    ADD_INT(d, DB_IMMUTABLE_KEY);
     ADD_INT(d, DB_READ_UNCOMMITTED);    /* replaces DB_DIRTY_READ in 4.4 */
     ADD_INT(d, DB_READ_COMMITTED);
 #endif
 
-#if (DBVER >= 33)
-    ADD_INT(d, DB_DONOTINDEX);
+#if (DBVER >= 44)
+    ADD_INT(d, DB_FREELIST_ONLY);
+    ADD_INT(d, DB_FREE_SPACE);
 #endif
 
-#if (DBVER >= 41)
-    _addIntToDict(d, "DB_INCOMPLETE", 0);
-#else
-    ADD_INT(d, DB_INCOMPLETE);
-#endif
+    ADD_INT(d, DB_DONOTINDEX);
+
     ADD_INT(d, DB_KEYEMPTY);
     ADD_INT(d, DB_KEYEXIST);
     ADD_INT(d, DB_LOCK_DEADLOCK);
     ADD_INT(d, DB_LOCK_NOTGRANTED);
     ADD_INT(d, DB_NOSERVER);
+#if (DBVER < 52)
     ADD_INT(d, DB_NOSERVER_HOME);
     ADD_INT(d, DB_NOSERVER_ID);
+#endif
     ADD_INT(d, DB_NOTFOUND);
     ADD_INT(d, DB_OLD_VERSION);
     ADD_INT(d, DB_RUNRECOVERY);
     ADD_INT(d, DB_VERIFY_BAD);
-#if (DBVER >= 33)
     ADD_INT(d, DB_PAGE_NOTFOUND);
     ADD_INT(d, DB_SECONDARY_BAD);
-#endif
-#if (DBVER >= 40)
     ADD_INT(d, DB_STAT_CLEAR);
     ADD_INT(d, DB_REGION_INIT);
     ADD_INT(d, DB_NOLOCKING);
     ADD_INT(d, DB_YIELDCPU);
     ADD_INT(d, DB_PANIC_ENVIRONMENT);
     ADD_INT(d, DB_NOPANIC);
+    ADD_INT(d, DB_OVERWRITE);
+
+    ADD_INT(d, DB_STAT_SUBSYSTEM);
+    ADD_INT(d, DB_STAT_MEMP_HASH);
+    ADD_INT(d, DB_STAT_LOCK_CONF);
+    ADD_INT(d, DB_STAT_LOCK_LOCKERS);
+    ADD_INT(d, DB_STAT_LOCK_OBJECTS);
+    ADD_INT(d, DB_STAT_LOCK_PARAMS);
+
+#if (DBVER >= 48)
+    ADD_INT(d, DB_OVERWRITE_DUP);
+#endif
+
+#if (DBVER >= 47)
+    ADD_INT(d, DB_FOREIGN_ABORT);
+    ADD_INT(d, DB_FOREIGN_CASCADE);
+    ADD_INT(d, DB_FOREIGN_NULLIFY);
+#endif
+
+#if (DBVER >= 44)
+    ADD_INT(d, DB_REGISTER);
 #endif
 
-#if (DBVER >= 42)
+    ADD_INT(d, DB_EID_INVALID);
+    ADD_INT(d, DB_EID_BROADCAST);
+
     ADD_INT(d, DB_TIME_NOTGRANTED);
     ADD_INT(d, DB_TXN_NOT_DURABLE);
     ADD_INT(d, DB_TXN_WRITE_NOSYNC);
-    ADD_INT(d, DB_LOG_AUTOREMOVE);
-    ADD_INT(d, DB_DIRECT_LOG);
     ADD_INT(d, DB_DIRECT_DB);
     ADD_INT(d, DB_INIT_REP);
     ADD_INT(d, DB_ENCRYPT);
     ADD_INT(d, DB_CHKSUM);
+
+#if (DBVER < 47)
+    ADD_INT(d, DB_LOG_AUTOREMOVE);
+    ADD_INT(d, DB_DIRECT_LOG);
+#endif
+
+#if (DBVER >= 47)
+    ADD_INT(d, DB_LOG_DIRECT);
+    ADD_INT(d, DB_LOG_DSYNC);
+    ADD_INT(d, DB_LOG_IN_MEMORY);
+    ADD_INT(d, DB_LOG_AUTO_REMOVE);
+    ADD_INT(d, DB_LOG_ZERO);
+#endif
+
+#if (DBVER >= 44)
+    ADD_INT(d, DB_DSYNC_DB);
+#endif
+
+#if (DBVER >= 45)
+    ADD_INT(d, DB_TXN_SNAPSHOT);
+#endif
+
+    ADD_INT(d, DB_VERB_DEADLOCK);
+#if (DBVER >= 46)
+    ADD_INT(d, DB_VERB_FILEOPS);
+    ADD_INT(d, DB_VERB_FILEOPS_ALL);
+#endif
+    ADD_INT(d, DB_VERB_RECOVERY);
+#if (DBVER >= 44)
+    ADD_INT(d, DB_VERB_REGISTER);
+#endif
+    ADD_INT(d, DB_VERB_REPLICATION);
+    ADD_INT(d, DB_VERB_WAITSFOR);
+
+#if (DBVER >= 50)
+    ADD_INT(d, DB_VERB_REP_SYSTEM);
+#endif
+
+#if (DBVER >= 47)
+    ADD_INT(d, DB_VERB_REP_ELECT);
+    ADD_INT(d, DB_VERB_REP_LEASE);
+    ADD_INT(d, DB_VERB_REP_MISC);
+    ADD_INT(d, DB_VERB_REP_MSGS);
+    ADD_INT(d, DB_VERB_REP_SYNC);
+    ADD_INT(d, DB_VERB_REPMGR_CONNFAIL);
+    ADD_INT(d, DB_VERB_REPMGR_MISC);
+#endif
+
+#if (DBVER >= 45)
+    ADD_INT(d, DB_EVENT_PANIC);
+    ADD_INT(d, DB_EVENT_REP_CLIENT);
+#if (DBVER >= 46)
+    ADD_INT(d, DB_EVENT_REP_ELECTED);
+#endif
+    ADD_INT(d, DB_EVENT_REP_MASTER);
+    ADD_INT(d, DB_EVENT_REP_NEWMASTER);
+#if (DBVER >= 46)
+    ADD_INT(d, DB_EVENT_REP_PERM_FAILED);
+#endif
+    ADD_INT(d, DB_EVENT_REP_STARTUPDONE);
+    ADD_INT(d, DB_EVENT_WRITE_FAILED);
+#endif
+
+#if (DBVER >= 50)
+    ADD_INT(d, DB_REPMGR_CONF_ELECTIONS);
+    ADD_INT(d, DB_EVENT_REP_MASTER_FAILURE);
+    ADD_INT(d, DB_EVENT_REP_DUPMASTER);
+    ADD_INT(d, DB_EVENT_REP_ELECTION_FAILED);
+#endif
+#if (DBVER >= 48)
+    ADD_INT(d, DB_EVENT_REG_ALIVE);
+    ADD_INT(d, DB_EVENT_REG_PANIC);
+#endif
+
+#if (DBVER >=52)
+    ADD_INT(d, DB_EVENT_REP_SITE_ADDED);
+    ADD_INT(d, DB_EVENT_REP_SITE_REMOVED);
+    ADD_INT(d, DB_EVENT_REP_LOCAL_SITE_REMOVED);
+    ADD_INT(d, DB_EVENT_REP_CONNECT_BROKEN);
+    ADD_INT(d, DB_EVENT_REP_CONNECT_ESTD);
+    ADD_INT(d, DB_EVENT_REP_CONNECT_TRY_FAILED);
+    ADD_INT(d, DB_EVENT_REP_INIT_DONE);
+
+    ADD_INT(d, DB_MEM_LOCK);
+    ADD_INT(d, DB_MEM_LOCKOBJECT);
+    ADD_INT(d, DB_MEM_LOCKER);
+    ADD_INT(d, DB_MEM_LOGID);
+    ADD_INT(d, DB_MEM_TRANSACTION);
+    ADD_INT(d, DB_MEM_THREAD);
+
+    ADD_INT(d, DB_BOOTSTRAP_HELPER);
+    ADD_INT(d, DB_GROUP_CREATOR);
+    ADD_INT(d, DB_LEGACY);
+    ADD_INT(d, DB_LOCAL_SITE);
+    ADD_INT(d, DB_REPMGR_PEER);
+#endif
+
+    ADD_INT(d, DB_REP_DUPMASTER);
+    ADD_INT(d, DB_REP_HOLDELECTION);
+#if (DBVER >= 44)
+    ADD_INT(d, DB_REP_IGNORE);
+    ADD_INT(d, DB_REP_JOIN_FAILURE);
+#endif
+    ADD_INT(d, DB_REP_ISPERM);
+    ADD_INT(d, DB_REP_NOTPERM);
+    ADD_INT(d, DB_REP_NEWSITE);
+
+    ADD_INT(d, DB_REP_MASTER);
+    ADD_INT(d, DB_REP_CLIENT);
+
+    ADD_INT(d, DB_REP_PERMANENT);
+
+#if (DBVER >= 44)
+#if (DBVER >= 50)
+    ADD_INT(d, DB_REP_CONF_AUTOINIT);
+#else
+    ADD_INT(d, DB_REP_CONF_NOAUTOINIT);
+#endif /* 5.0 */
+#endif /* 4.4 */
+#if (DBVER >= 44)
+    ADD_INT(d, DB_REP_CONF_DELAYCLIENT);
+    ADD_INT(d, DB_REP_CONF_BULK);
+    ADD_INT(d, DB_REP_CONF_NOWAIT);
+    ADD_INT(d, DB_REP_ANYWHERE);
+    ADD_INT(d, DB_REP_REREQUEST);
+#endif
+
+    ADD_INT(d, DB_REP_NOBUFFER);
+
+#if (DBVER >= 46)
+    ADD_INT(d, DB_REP_LEASE_EXPIRED);
+    ADD_INT(d, DB_IGNORE_LEASE);
+#endif
+
+#if (DBVER >= 47)
+    ADD_INT(d, DB_REP_CONF_LEASE);
+    ADD_INT(d, DB_REPMGR_CONF_2SITE_STRICT);
+#endif
+
+#if (DBVER >= 45)
+    ADD_INT(d, DB_REP_ELECTION);
+
+    ADD_INT(d, DB_REP_ACK_TIMEOUT);
+    ADD_INT(d, DB_REP_CONNECTION_RETRY);
+    ADD_INT(d, DB_REP_ELECTION_TIMEOUT);
+    ADD_INT(d, DB_REP_ELECTION_RETRY);
+#endif
+#if (DBVER >= 46)
+    ADD_INT(d, DB_REP_CHECKPOINT_DELAY);
+    ADD_INT(d, DB_REP_FULL_ELECTION_TIMEOUT);
+    ADD_INT(d, DB_REP_LEASE_TIMEOUT);
+#endif
+#if (DBVER >= 47)
+    ADD_INT(d, DB_REP_HEARTBEAT_MONITOR);
+    ADD_INT(d, DB_REP_HEARTBEAT_SEND);
+#endif
+
+#if (DBVER >= 45)
+    ADD_INT(d, DB_REPMGR_PEER);
+    ADD_INT(d, DB_REPMGR_ACKS_ALL);
+    ADD_INT(d, DB_REPMGR_ACKS_ALL_PEERS);
+    ADD_INT(d, DB_REPMGR_ACKS_NONE);
+    ADD_INT(d, DB_REPMGR_ACKS_ONE);
+    ADD_INT(d, DB_REPMGR_ACKS_ONE_PEER);
+    ADD_INT(d, DB_REPMGR_ACKS_QUORUM);
+    ADD_INT(d, DB_REPMGR_CONNECTED);
+    ADD_INT(d, DB_REPMGR_DISCONNECTED);
+    ADD_INT(d, DB_STAT_ALL);
+#endif
+
+#if (DBVER >= 51)
+    ADD_INT(d, DB_REPMGR_ACKS_ALL_AVAILABLE);
+#endif
+
+#if (DBVER >= 48)
+    ADD_INT(d, DB_REP_CONF_INMEM);
+#endif
+
+    ADD_INT(d, DB_TIMEOUT);
+
+#if (DBVER >= 50)
+    ADD_INT(d, DB_FORCESYNC);
+#endif
+
+#if (DBVER >= 48)
+    ADD_INT(d, DB_FAILCHK);
+#endif
+
+#if (DBVER >= 51)
+    ADD_INT(d, DB_HOTBACKUP_IN_PROGRESS);
 #endif
 
-#if (DBVER >= 43)
-    ADD_INT(d, DB_LOG_INMEMORY);
     ADD_INT(d, DB_BUFFER_SMALL);
     ADD_INT(d, DB_SEQ_DEC);
     ADD_INT(d, DB_SEQ_INC);
     ADD_INT(d, DB_SEQ_WRAP);
+
+#if (DBVER < 47)
+    ADD_INT(d, DB_LOG_INMEMORY);
+    ADD_INT(d, DB_DSYNC_LOG);
 #endif
 
-#if (DBVER >= 41)
     ADD_INT(d, DB_ENCRYPT_AES);
     ADD_INT(d, DB_AUTO_COMMIT);
-#else
-    /* allow berkeleydb 4.1 aware apps to run on older versions */
-    _addIntToDict(d, "DB_AUTO_COMMIT", 0);
+    ADD_INT(d, DB_PRIORITY_VERY_LOW);
+    ADD_INT(d, DB_PRIORITY_LOW);
+    ADD_INT(d, DB_PRIORITY_DEFAULT);
+    ADD_INT(d, DB_PRIORITY_HIGH);
+    ADD_INT(d, DB_PRIORITY_VERY_HIGH);
+
+#if (DBVER >= 46)
+    ADD_INT(d, DB_PRIORITY_UNCHANGED);
 #endif
 
     ADD_INT(d, EINVAL);
@@ -5993,9 +10182,11 @@
     ADD_INT(d, ENOENT);
     ADD_INT(d, EPERM);
 
-#if (DBVER >= 40)
     ADD_INT(d, DB_SET_LOCK_TIMEOUT);
     ADD_INT(d, DB_SET_TXN_TIMEOUT);
+
+#if (DBVER >= 48)
+    ADD_INT(d, DB_SET_REG_TIMEOUT);
 #endif
 
     /* The exception name must be correct for pickled exception *
@@ -6014,23 +10205,39 @@
     DBError = NULL;     /* used in MAKE_EX so that it derives from nothing */
     MAKE_EX(DBError);
 
+#if (PY_VERSION_HEX < 0x03000000)
     /* Some magic to make DBNotFoundError and DBKeyEmptyError derive
      * from both DBError and KeyError, since the API only supports
      * using one base class. */
     PyDict_SetItemString(d, "KeyError", PyExc_KeyError);
     PyRun_String("class DBNotFoundError(DBError, KeyError): pass\n"
-	         "class DBKeyEmptyError(DBError, KeyError): pass",
+                 "class DBKeyEmptyError(DBError, KeyError): pass",
                  Py_file_input, d, d);
     DBNotFoundError = PyDict_GetItemString(d, "DBNotFoundError");
     DBKeyEmptyError = PyDict_GetItemString(d, "DBKeyEmptyError");
     PyDict_DelItemString(d, "KeyError");
+#else
+    /* Since Python 2.5, PyErr_NewException() accepts a tuple, to be able to
+    ** derive from several classes. We use this new API only for Python 3.0,
+    ** though.
+    */
+    {
+        PyObject* bases;
+
+        bases = PyTuple_Pack(2, DBError, PyExc_KeyError);
+
+#define MAKE_EX2(name)   name = PyErr_NewException(PYBSDDB_EXCEPTION_BASE #name, bases, NULL); \
+                         PyDict_SetItemString(d, #name, name)
+        MAKE_EX2(DBNotFoundError);
+        MAKE_EX2(DBKeyEmptyError);
 
+#undef MAKE_EX2
 
-#if !INCOMPLETE_IS_WARNING
-    MAKE_EX(DBIncompleteError);
+        Py_XDECREF(bases);
+    }
 #endif
+
     MAKE_EX(DBCursorClosedError);
-    MAKE_EX(DBKeyEmptyError);
     MAKE_EX(DBKeyExistError);
     MAKE_EX(DBLockDeadlockError);
     MAKE_EX(DBLockNotGrantedError);
@@ -6038,12 +10245,12 @@
     MAKE_EX(DBRunRecoveryError);
     MAKE_EX(DBVerifyBadError);
     MAKE_EX(DBNoServerError);
+#if (DBVER < 52)
     MAKE_EX(DBNoServerHomeError);
     MAKE_EX(DBNoServerIDError);
-#if (DBVER >= 33)
+#endif
     MAKE_EX(DBPageNotFoundError);
     MAKE_EX(DBSecondaryBadError);
-#endif
 
     MAKE_EX(DBInvalidArgError);
     MAKE_EX(DBAccessError);
@@ -6055,20 +10262,101 @@
     MAKE_EX(DBNoSuchFileError);
     MAKE_EX(DBPermissionsError);
 
+    MAKE_EX(DBRepHandleDeadError);
+#if (DBVER >= 44)
+    MAKE_EX(DBRepLockoutError);
+#endif
+
+    MAKE_EX(DBRepUnavailError);
+
+#if (DBVER >= 46)
+    MAKE_EX(DBRepLeaseExpiredError);
+#endif
+
+#if (DBVER >= 47)
+        MAKE_EX(DBForeignConflictError);
+#endif
+
 #undef MAKE_EX
 
+    /* Initialise the C API structure and add it to the module */
+    bsddb_api.api_version      = PYBSDDB_API_VERSION;
+    bsddb_api.db_type          = &DB_Type;
+    bsddb_api.dbcursor_type    = &DBCursor_Type;
+    bsddb_api.dblogcursor_type = &DBLogCursor_Type;
+    bsddb_api.dbenv_type       = &DBEnv_Type;
+    bsddb_api.dbtxn_type       = &DBTxn_Type;
+    bsddb_api.dblock_type      = &DBLock_Type;
+    bsddb_api.dbsequence_type  = &DBSequence_Type;
+    bsddb_api.makeDBError      = makeDBError;
+
+    /*
+    ** Capsules exist from Python 2.7 and 3.1.
+    ** We don't support Python 3.0 anymore, so...
+    ** #if (PY_VERSION_HEX < ((PY_MAJOR_VERSION < 3) ? 0x02070000 : 0x03020000))
+    */
+#if (PY_VERSION_HEX < 0x02070000)
+    py_api = PyCObject_FromVoidPtr((void*)&bsddb_api, NULL);
+#else
+    {
+        /*
+        ** The data must outlive the call!!. So, the static definition.
+        ** The buffer must be big enough...
+        */
+        static char py_api_name[MODULE_NAME_MAX_LEN+10];
+
+        strcpy(py_api_name, _bsddbModuleName);
+        strcat(py_api_name, ".api");
+
+        py_api = PyCapsule_New((void*)&bsddb_api, py_api_name, NULL);
+    }
+#endif
+
+    /* Check error control */
+    /*
+    ** PyErr_NoMemory();
+    ** py_api = NULL;
+    */
+
+    if (py_api) {
+        PyDict_SetItemString(d, "api", py_api);
+        Py_DECREF(py_api);
+    } else { /* Something bad happened */
+        PyErr_WriteUnraisable(m);
+        if(PyErr_Warn(PyExc_RuntimeWarning,
+                "_bsddb/_pybsddb C API will be not available")) {
+            PyErr_WriteUnraisable(m);
+        }
+        PyErr_Clear();
+    }
+
     /* Check for errors */
     if (PyErr_Occurred()) {
         PyErr_Print();
-        Py_FatalError("can't initialize module _bsddb");
+        Py_FatalError("can't initialize module _bsddb/_pybsddb");
+        Py_DECREF(m);
+        m = NULL;
     }
+#if (PY_VERSION_HEX < 0x03000000)
+    return;
+#else
+    return m;
+#endif
 }
 
 /* allow this module to be named _pybsddb so that it can be installed
  * and imported on top of python >= 2.3 that includes its own older
  * copy of the library named _bsddb without importing the old version. */
+#if (PY_VERSION_HEX < 0x03000000)
 DL_EXPORT(void) init_pybsddb(void)
+#else
+PyMODINIT_FUNC PyInit__pybsddb(void)  /* Note the two underscores */
+#endif
 {
     strncpy(_bsddbModuleName, "_pybsddb", MODULE_NAME_MAX_LEN);
+#if (PY_VERSION_HEX < 0x03000000)
     init_bsddb();
+#else
+    return PyInit__bsddb();   /* Note the two underscores */
+#endif
 }
--- /dev/null
+++ b/Modules/bsddb.h
@@ -0,0 +1,315 @@
+/*----------------------------------------------------------------------
+  Copyright (c) 1999-2001, Digital Creations, Fredericksburg, VA, USA
+  and Andrew Kuchling. All rights reserved.
+
+  Redistribution and use in source and binary forms, with or without
+  modification, are permitted provided that the following conditions are
+  met:
+
+    o Redistributions of source code must retain the above copyright
+      notice, this list of conditions, and the disclaimer that follows.
+
+    o Redistributions in binary form must reproduce the above copyright
+      notice, this list of conditions, and the following disclaimer in
+      the documentation and/or other materials provided with the
+      distribution.
+
+    o Neither the name of Digital Creations nor the names of its
+      contributors may be used to endorse or promote products derived
+      from this software without specific prior written permission.
+
+  THIS SOFTWARE IS PROVIDED BY DIGITAL CREATIONS AND CONTRIBUTORS *AS
+  IS* AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
+  TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
+  PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL DIGITAL
+  CREATIONS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
+  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
+  BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
+  OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
+  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
+  TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
+  USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
+  DAMAGE.
+------------------------------------------------------------------------*/
+
+
+/*
+ * Handwritten code to wrap version 3.x of the Berkeley DB library,
+ * written to replace a SWIG-generated file.  It has since been updated
+ * to compile with Berkeley DB versions 3.2 through 4.2.
+ *
+ * This module was started by Andrew Kuchling to remove the dependency
+ * on SWIG in a package by Gregory P. Smith who based his work on a
+ * similar package by Robin Dunn <robin@alldunn.com> which wrapped
+ * Berkeley DB 2.7.x.
+ *
+ * Development of this module then returned full circle back to Robin Dunn
+ * who worked on behalf of Digital Creations to complete the wrapping of
+ * the DB 3.x API and to build a solid unit test suite.  Robin has
+ * since gone onto other projects (wxPython).
+ *
+ * Gregory P. Smith <greg@krypto.org> is once again the maintainer.
+ *
+ * Use the pybsddb-users@lists.sf.net mailing list for all questions.
+ * Things can change faster than the header of this file is updated.  This
+ * file is shared with the PyBSDDB project at SourceForge:
+ *
+ * http://pybsddb.sf.net
+ *
+ * This file should remain backward compatible with Python 2.1, but see PEP
+ * 291 for the most current backward compatibility requirements:
+ *
+ * http://www.python.org/peps/pep-0291.html
+ *
+ * This module contains 7 types:
+ *
+ * DB           (Database)
+ * DBCursor     (Database Cursor)
+ * DBEnv        (database environment)
+ * DBTxn        (An explicit database transaction)
+ * DBLock       (A lock handle)
+ * DBSequence   (Sequence)
+ * DBSite       (Site)
+ *
+ * New datatypes:
+ *
+ * DBLogCursor  (Log Cursor)
+ *
+ */
+
+/* --------------------------------------------------------------------- */
+
+/*
+ * Portions of this module, associated unit tests and build scripts are the
+ * result of a contract with The Written Word (http://thewrittenword.com/)
+ * Many thanks go out to them for causing me to raise the bar on quality and
+ * functionality, resulting in a better bsddb3 package for all of us to use.
+ *
+ * --Robin
+ */
+
+/* --------------------------------------------------------------------- */
+
+/*
+ * Work to split it up into a separate header and to add a C API was
+ * contributed by Duncan Grisby <duncan@tideway.com>.   See here:
+ *  http://sourceforge.net/tracker/index.php?func=detail&aid=1551895&group_id=13900&atid=313900
+ */
+
+/* --------------------------------------------------------------------- */
+
+#ifndef _BSDDB_H_
+#define _BSDDB_H_
+
+#include <db.h>
+
+
+/* 40 = 4.0, 33 = 3.3; this will break if the minor revision is > 9 */
+#define DBVER (DB_VERSION_MAJOR * 10 + DB_VERSION_MINOR)
+#if DB_VERSION_MINOR > 9
+#error "eek! DBVER can't handle minor versions > 9"
+#endif
+
+#define PY_BSDDB_VERSION "5.3.0"
+
+/* Python object definitions */
+
+struct behaviourFlags {
+    /* What is the default behaviour when DB->get or DBCursor->get returns a
+       DB_NOTFOUND || DB_KEYEMPTY error?  Return None or raise an exception? */
+    unsigned int getReturnsNone : 1;
+    /* What is the default behaviour for DBCursor.set* methods when DBCursor->get
+     * returns a DB_NOTFOUND || DB_KEYEMPTY  error?  Return None or raise? */
+    unsigned int cursorSetReturnsNone : 1;
+};
+
+
+
+struct DBObject;          /* Forward declaration */
+struct DBCursorObject;    /* Forward declaration */
+struct DBLogCursorObject; /* Forward declaration */
+struct DBTxnObject;       /* Forward declaration */
+struct DBSequenceObject;  /* Forward declaration */
+#if (DBVER >= 52)
+struct DBSiteObject;      /* Forward declaration */
+#endif
+
+typedef struct {
+    PyObject_HEAD
+    DB_ENV*     db_env;
+    u_int32_t   flags;             /* saved flags from open() */
+    int         closed;
+    struct behaviourFlags moduleFlags;
+    PyObject*       event_notifyCallback;
+    struct DBObject *children_dbs;
+    struct DBTxnObject *children_txns;
+    struct DBLogCursorObject *children_logcursors;
+#if (DBVER >= 52)
+    struct DBSiteObject *children_sites;
+#endif
+    PyObject        *private_obj;
+    PyObject        *rep_transport;
+    PyObject        *in_weakreflist; /* List of weak references */
+} DBEnvObject;
+
+typedef struct DBObject {
+    PyObject_HEAD
+    DB*             db;
+    DBEnvObject*    myenvobj;  /* PyObject containing the DB_ENV */
+    u_int32_t       flags;     /* saved flags from open() */
+    u_int32_t       setflags;  /* saved flags from set_flags() */
+    struct behaviourFlags moduleFlags;
+    struct DBTxnObject *txn;
+    struct DBCursorObject *children_cursors;
+    struct DBSequenceObject *children_sequences;
+    struct DBObject **sibling_prev_p;
+    struct DBObject *sibling_next;
+    struct DBObject **sibling_prev_p_txn;
+    struct DBObject *sibling_next_txn;
+    PyObject*       associateCallback;
+    PyObject*       btCompareCallback;
+    PyObject*       dupCompareCallback;	    
+    int             primaryDBType;
+    PyObject        *private_obj;
+    PyObject        *in_weakreflist; /* List of weak references */
+} DBObject;
+
+
+typedef struct DBCursorObject {
+    PyObject_HEAD
+    DBC*            dbc;
+    struct DBCursorObject **sibling_prev_p;
+    struct DBCursorObject *sibling_next;
+    struct DBCursorObject **sibling_prev_p_txn;
+    struct DBCursorObject *sibling_next_txn;
+    DBObject*       mydb;
+    struct DBTxnObject *txn;
+    PyObject        *in_weakreflist; /* List of weak references */
+} DBCursorObject;
+
+
+typedef struct DBTxnObject {
+    PyObject_HEAD
+    DB_TXN*         txn;
+    DBEnvObject*    env;
+    int             flag_prepare;
+    struct DBTxnObject *parent_txn;
+    struct DBTxnObject **sibling_prev_p;
+    struct DBTxnObject *sibling_next;
+    struct DBTxnObject *children_txns;
+    struct DBObject *children_dbs;
+    struct DBSequenceObject *children_sequences;
+    struct DBCursorObject *children_cursors;
+    PyObject        *in_weakreflist; /* List of weak references */
+} DBTxnObject;
+
+
+typedef struct DBLogCursorObject {
+    PyObject_HEAD
+    DB_LOGC*        logc;
+    DBEnvObject*    env;
+    struct DBLogCursorObject **sibling_prev_p;
+    struct DBLogCursorObject *sibling_next;
+    PyObject        *in_weakreflist; /* List of weak references */
+} DBLogCursorObject;
+
+#if (DBVER >= 52)
+typedef struct DBSiteObject {
+    PyObject_HEAD
+    DB_SITE         *site;
+    DBEnvObject     *env;
+    struct DBSiteObject **sibling_prev_p;
+    struct DBSiteObject *sibling_next;
+    PyObject    *in_weakreflist; /* List of weak references */
+} DBSiteObject;
+#endif
+
+typedef struct {
+    PyObject_HEAD
+    DB_LOCK         lock;
+    int             lock_initialized;  /* Signal if we actually have a lock */
+    PyObject        *in_weakreflist; /* List of weak references */
+} DBLockObject;
+
+
+typedef struct DBSequenceObject {
+    PyObject_HEAD
+    DB_SEQUENCE*     sequence;
+    DBObject*        mydb;
+    struct DBTxnObject *txn;
+    struct DBSequenceObject **sibling_prev_p;
+    struct DBSequenceObject *sibling_next;
+    struct DBSequenceObject **sibling_prev_p_txn;
+    struct DBSequenceObject *sibling_next_txn;
+    PyObject        *in_weakreflist; /* List of weak references */
+} DBSequenceObject;
+
+
+/* API structure for use by C code */
+
+/* To access the structure from an external module, use code like the
+   following (error checking missed out for clarity):
+
+     // If you are using Python before 2.7:
+     BSDDB_api* bsddb_api;
+     PyObject*  mod;
+     PyObject*  cobj;
+
+     mod  = PyImport_ImportModule("bsddb._bsddb");
+     // Use "bsddb3._pybsddb" if you're using the standalone pybsddb add-on.
+     cobj = PyObject_GetAttrString(mod, "api");
+     api  = (BSDDB_api*)PyCObject_AsVoidPtr(cobj);
+     Py_DECREF(cobj);
+     Py_DECREF(mod);
+
+
+     // If you are using Python 2.7 or up: (except Python 3.0, unsupported)
+     BSDDB_api* bsddb_api;
+
+     // Use "bsddb3._pybsddb.api" if you're using
+     // the standalone pybsddb add-on.
+     bsddb_api = (void **)PyCapsule_Import("bsddb._bsddb.api", 1);
+
+
+   Check "api_version" number before trying to use the API.
+
+   The structure's members must not be changed.
+*/
+
+#define PYBSDDB_API_VERSION 1
+typedef struct {
+    unsigned int api_version;
+    /* Type objects */
+    PyTypeObject* db_type;
+    PyTypeObject* dbcursor_type;
+    PyTypeObject* dblogcursor_type;
+    PyTypeObject* dbenv_type;
+    PyTypeObject* dbtxn_type;
+    PyTypeObject* dblock_type;
+    PyTypeObject* dbsequence_type;
+
+    /* Functions */
+    int (*makeDBError)(int err);
+} BSDDB_api;
+
+
+#ifndef COMPILING_BSDDB_C
+
+/* If not inside _bsddb.c, define type check macros that use the api
+   structure.  The calling code must have a value named bsddb_api
+   pointing to the api structure.
+*/
+
+#define DBObject_Check(v)       ((v)->ob_type == bsddb_api->db_type)
+#define DBCursorObject_Check(v) ((v)->ob_type == bsddb_api->dbcursor_type)
+#define DBEnvObject_Check(v)    ((v)->ob_type == bsddb_api->dbenv_type)
+#define DBTxnObject_Check(v)    ((v)->ob_type == bsddb_api->dbtxn_type)
+#define DBLockObject_Check(v)   ((v)->ob_type == bsddb_api->dblock_type)
+#define DBSequenceObject_Check(v)  \
+    ((bsddb_api->dbsequence_type) && \
+        ((v)->ob_type == bsddb_api->dbsequence_type))
+
+#endif /* COMPILING_BSDDB_C */
+
+
+#endif /* _BSDDB_H_ */
