from __future__ import annotations

import textwrap

REMOTE_RSYNC_PROBE_CODE = "import shutil, sys; sys.exit(0 if shutil.which('rsync') else 1)"

REMOTE_PREPARE_RSYNC_CODE = textwrap.dedent(
    r"""
    import tempfile
    print(tempfile.mkdtemp(prefix="remote-sandbox-rsync-"), flush=True)
    """
).strip()

REMOTE_DELETE_EXPECTED_CODE = textwrap.dedent(
    r"""
    import hashlib
    import json
    import os
    import secrets
    import stat
    import sys

    def open_dir(path, parent_fd=None):
        flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
        return os.open(path, flags, dir_fd=parent_fd)

    def valid(name):
        parts = name.split("/")
        if (
            not name
            or name.startswith("/")
            or "\\" in name
            or any(ord(char) < 32 or ord(char) == 127 for char in name)
            or any(part in {"", ".", ".."} for part in parts)
        ):
            raise ValueError("invalid delete path")
        return parts

    def open_parent(root_fd, parts):
        descriptor = os.dup(root_fd)
        try:
            for part in parts:
                child = open_dir(part, descriptor)
                os.close(descriptor)
                descriptor = child
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def fingerprint(parent_fd, leaf, path):
        try:
            entry = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return {"path": path, "missing": True}
        if stat.S_ISLNK(entry.st_mode):
            target = os.readlink(leaf, dir_fd=parent_fd)
            return {
                "path": path,
                "missing": False,
                "kind": "symlink",
                "size": None,
                "mtime_ns": entry.st_mtime_ns,
                "mode": entry.st_mode,
                "link_target": target,
                "content_hash": hashlib.sha256(os.fsencode(target)).hexdigest(),
            }
        if stat.S_ISDIR(entry.st_mode):
            return {
                "path": path,
                "missing": False,
                "kind": "dir",
                "size": None,
                "mtime_ns": entry.st_mtime_ns,
                "mode": entry.st_mode,
                "link_target": None,
                "content_hash": None,
            }
        if not stat.S_ISREG(entry.st_mode):
            return {
                "path": path,
                "missing": False,
                "kind": "special",
                "size": None,
                "mtime_ns": entry.st_mtime_ns,
                "mode": entry.st_mode,
                "link_target": None,
                "content_hash": None,
            }
        descriptor = os.open(
            leaf,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_fd,
        )
        digest = hashlib.sha256()
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (entry.st_dev, entry.st_ino):
                raise RuntimeError("delete candidate changed while opening")
            with os.fdopen(descriptor, "rb", closefd=False) as source:
                while True:
                    block = source.read(1024 * 1024)
                    if not block:
                        break
                    digest.update(block)
            after = os.fstat(descriptor)
            if (
                after.st_dev,
                after.st_ino,
                after.st_mode,
                after.st_size,
                after.st_mtime_ns,
            ) != (
                entry.st_dev,
                entry.st_ino,
                entry.st_mode,
                entry.st_size,
                entry.st_mtime_ns,
            ):
                raise RuntimeError("delete candidate changed while hashing")
        finally:
            os.close(descriptor)
        return {
            "path": path,
            "missing": False,
            "kind": "file",
            "size": entry.st_size,
            "mtime_ns": entry.st_mtime_ns,
            "mode": entry.st_mode,
            "link_target": None,
            "content_hash": digest.hexdigest(),
        }

    def matches(expected, observed):
        if expected == observed:
            return True
        return (
            not expected.get("missing", False)
            and not observed.get("missing", False)
            and expected.get("kind") == "dir"
            and observed.get("kind") == "dir"
            and expected.get("mode") == observed.get("mode")
        )

    def restore(parent_fd, quarantine, leaf):
        try:
            os.stat(quarantine, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        try:
            os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            os.rename(quarantine, leaf, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
            return
        recovery = ".remote-sandbox-recovered-" + secrets.token_hex(8)
        os.rename(quarantine, recovery, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
        raise RuntimeError("concurrent replacement preserved as " + recovery)

    request = json.load(sys.stdin)
    entries = request.get("entries")
    if not isinstance(entries, list):
        raise ValueError("delete entries must be a list")
    root_fd = open_dir(sys.argv[1])
    completed = []
    changed = []
    try:
        for expected in entries:
            if not isinstance(expected, dict) or not isinstance(expected.get("path"), str):
                raise ValueError("invalid expected delete entry")
            path = expected["path"]
            parts = valid(path)
            try:
                parent_fd = open_parent(root_fd, parts[:-1])
            except FileNotFoundError:
                if expected.get("missing") is True:
                    completed.append(path)
                else:
                    changed.append(path)
                continue
            quarantine = ".remote-sandbox-delete-" + secrets.token_hex(8)
            try:
                try:
                    os.rename(
                        parts[-1],
                        quarantine,
                        src_dir_fd=parent_fd,
                        dst_dir_fd=parent_fd,
                    )
                except FileNotFoundError:
                    if expected.get("missing") is True:
                        completed.append(path)
                    else:
                        changed.append(path)
                    continue
                try:
                    observed = fingerprint(parent_fd, quarantine, path)
                    if not matches(expected, observed):
                        restore(parent_fd, quarantine, parts[-1])
                        changed.append(path)
                        continue
                    entry = os.stat(quarantine, dir_fd=parent_fd, follow_symlinks=False)
                    if stat.S_ISDIR(entry.st_mode) and not stat.S_ISLNK(entry.st_mode):
                        os.rmdir(quarantine, dir_fd=parent_fd)
                    else:
                        os.unlink(quarantine, dir_fd=parent_fd)
                    completed.append(path)
                except BaseException:
                    restore(parent_fd, quarantine, parts[-1])
                    raise
            finally:
                os.close(parent_fd)
    finally:
        os.close(root_fd)
    print(json.dumps({"completed": completed, "changed": changed}, separators=(",", ":")))
    """
).strip()

REMOTE_STAGE_RSYNC_CODE = textwrap.dedent(
    r"""
    import os
    import shutil
    import stat
    import sys
    import tempfile

    def valid(name):
        parts = name.split("/")
        if (
            not name
            or name.startswith("/")
            or "\\" in name
            or any(ord(char) < 32 or ord(char) == 127 for char in name)
            or any(part in {"", ".", ".."} for part in parts)
        ):
            raise ValueError("invalid transfer path")
        return parts

    def open_dir(parent_fd, name):
        flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
        return os.open(name, flags, dir_fd=parent_fd)

    def copy_entry(parent_fd, leaf, destination):
        entry = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
        os.makedirs(os.path.dirname(destination), mode=0o700, exist_ok=True)
        if stat.S_ISDIR(entry.st_mode):
            os.makedirs(destination, mode=stat.S_IMODE(entry.st_mode), exist_ok=True)
            os.chmod(destination, stat.S_IMODE(entry.st_mode))
            directories.append(
                (destination, entry.st_atime_ns, entry.st_mtime_ns, entry.st_mode)
            )
            return
        if stat.S_ISLNK(entry.st_mode):
            os.symlink(os.readlink(leaf, dir_fd=parent_fd), destination)
            os.utime(
                destination,
                ns=(entry.st_atime_ns, entry.st_mtime_ns),
                follow_symlinks=False,
            )
            return
        if not stat.S_ISREG(entry.st_mode):
            raise ValueError("special files are not transferable")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(leaf, flags, dir_fd=parent_fd)
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (entry.st_dev, entry.st_ino):
                raise RuntimeError("source changed while opening")
            with os.fdopen(descriptor, "rb", closefd=False) as source:
                with open(destination, "xb") as output:
                    shutil.copyfileobj(source, output, length=1024 * 1024)
            os.chmod(destination, stat.S_IMODE(opened.st_mode))
            os.utime(
                destination,
                ns=(opened.st_atime_ns, opened.st_mtime_ns),
                follow_symlinks=False,
            )
        finally:
            os.close(descriptor)

    root = sys.argv[1]
    paths = sys.argv[2:]
    root_fd = open_dir(None, root)
    staging = tempfile.mkdtemp(prefix="remote-sandbox-rsync-")
    directories = []
    try:
        for relative in paths:
            parts = valid(relative)
            descriptor = os.dup(root_fd)
            try:
                for part in parts[:-1]:
                    child = open_dir(descriptor, part)
                    os.close(descriptor)
                    descriptor = child
                copy_entry(descriptor, parts[-1], os.path.join(staging, *parts))
            finally:
                os.close(descriptor)
        for destination, atime_ns, mtime_ns, mode in sorted(
            directories,
            key=lambda item: item[0].count(os.sep),
            reverse=True,
        ):
            os.chmod(destination, stat.S_IMODE(mode))
            os.utime(
                destination,
                ns=(atime_ns, mtime_ns),
                follow_symlinks=False,
            )
        print(staging, flush=True)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    finally:
        os.close(root_fd)
    """
).strip()

REMOTE_FINALIZE_RSYNC_CODE = textwrap.dedent(
    r"""
    import errno
    import os
    import secrets
    import shutil
    import stat
    import sys
    import tempfile

    def valid(name):
        parts = name.split("/")
        if (
            not name
            or name.startswith("/")
            or "\\" in name
            or any(ord(char) < 32 or ord(char) == 127 for char in name)
            or any(part in {"", ".", ".."} for part in parts)
        ):
            raise ValueError("invalid transfer path")
        return parts

    def open_dir(path, parent_fd=None):
        flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
        return os.open(path, flags, dir_fd=parent_fd)

    def open_parent(root_fd, parts):
        descriptor = os.dup(root_fd)
        try:
            for part in parts:
                try:
                    child = open_dir(part, descriptor)
                except FileNotFoundError:
                    os.mkdir(part, 0o700, dir_fd=descriptor)
                    child = open_dir(part, descriptor)
                os.close(descriptor)
                descriptor = child
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def remove_at(parent_fd, name):
        try:
            entry = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        if stat.S_ISDIR(entry.st_mode) and not stat.S_ISLNK(entry.st_mode):
            descriptor = open_dir(name, parent_fd)
            try:
                for child in os.listdir(descriptor):
                    remove_at(descriptor, child)
            finally:
                os.close(descriptor)
            os.rmdir(name, dir_fd=parent_fd)
        else:
            os.unlink(name, dir_fd=parent_fd)

    def copy_to_parent(source, parent_fd, name):
        entry = os.lstat(source)
        if stat.S_ISLNK(entry.st_mode):
            os.symlink(os.readlink(source), name, dir_fd=parent_fd)
            return
        if stat.S_ISREG(entry.st_mode):
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(name, flags, stat.S_IMODE(entry.st_mode), dir_fd=parent_fd)
            try:
                with open(source, "rb") as input_file:
                    with os.fdopen(descriptor, "wb", closefd=False) as output:
                        shutil.copyfileobj(input_file, output, length=1024 * 1024)
            finally:
                os.close(descriptor)
            os.chmod(
                name,
                stat.S_IMODE(entry.st_mode),
                dir_fd=parent_fd,
                follow_symlinks=False,
            )
            return
        os.mkdir(name, stat.S_IMODE(entry.st_mode), dir_fd=parent_fd)
        os.chmod(
            name,
            stat.S_IMODE(entry.st_mode),
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        descriptor = open_dir(name, parent_fd)
        try:
            for child in os.listdir(source):
                copy_to_parent(os.path.join(source, child), descriptor, child)
        finally:
            os.close(descriptor)

    root = sys.argv[1]
    staging = os.path.realpath(sys.argv[2])
    paths = sys.argv[3:]
    temp_root = os.path.realpath(tempfile.gettempdir())
    if os.path.dirname(staging) != temp_root or not os.path.basename(staging).startswith(
        "remote-sandbox-rsync-"
    ):
        raise ValueError("invalid rsync staging root")
    root_fd = open_dir(root)
    try:
        top_level = [
            path
            for path in paths
            if not any(other != path and path.startswith(other + "/") for other in paths)
        ]
        for relative in top_level:
            parts = valid(relative)
            parent_fd = open_parent(root_fd, parts[:-1])
            temporary = ".remote-sandbox-new-" + secrets.token_hex(8)
            backup = ".remote-sandbox-old-" + secrets.token_hex(8)
            source = os.path.join(staging, *parts)
            try:
                try:
                    os.rename(source, temporary, dst_dir_fd=parent_fd)
                except OSError as exc:
                    if exc.errno != errno.EXDEV:
                        raise
                    copy_to_parent(source, parent_fd, temporary)
                had_destination = False
                try:
                    os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
                    had_destination = True
                    os.rename(
                        parts[-1], backup, src_dir_fd=parent_fd, dst_dir_fd=parent_fd
                    )
                except FileNotFoundError:
                    pass
                try:
                    os.rename(
                        temporary, parts[-1], src_dir_fd=parent_fd, dst_dir_fd=parent_fd
                    )
                except BaseException:
                    if had_destination:
                        os.rename(
                            backup,
                            parts[-1],
                            src_dir_fd=parent_fd,
                            dst_dir_fd=parent_fd,
                        )
                    raise
                if had_destination:
                    remove_at(parent_fd, backup)
            finally:
                os.close(parent_fd)
    finally:
        os.close(root_fd)
        shutil.rmtree(staging, ignore_errors=True)
    """
).strip()

REMOTE_CLEANUP_RSYNC_CODE = textwrap.dedent(
    r"""
    import os
    import shutil
    import sys
    import tempfile
    staging = os.path.realpath(sys.argv[2])
    temp_root = os.path.realpath(tempfile.gettempdir())
    if os.path.dirname(staging) != temp_root or not os.path.basename(staging).startswith(
        "remote-sandbox-rsync-"
    ):
        raise ValueError("invalid rsync staging root")
    shutil.rmtree(staging)
    if os.path.lexists(staging):
        raise RuntimeError("rsync staging cleanup left residue")
    """
).strip()

REMOTE_CREATE_CODE = textwrap.dedent(
    r"""
    import os
    import stat
    import sys
    import tarfile

    def valid(name):
        if not name or name in {".", ".."} or name.startswith("/") or "\\" in name:
            raise ValueError("invalid transfer path")
        if any(ord(char) < 32 or ord(char) == 127 for char in name):
            raise ValueError("invalid transfer path")
        parts = name.split("/")
        if any(part in {"", ".", ".."} for part in parts):
            raise ValueError("invalid transfer path")
        return parts

    def open_dir(parent_fd, name):
        flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
        return os.open(name, flags, dir_fd=parent_fd)

    root = sys.argv[1]
    requested = sys.argv[2:]
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
    seen = set()

    with tarfile.open(fileobj=sys.stdout.buffer, mode="w|") as archive:
        def add(parent_fd, leaf, archive_name):
            if archive_name in seen:
                return
            entry = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
            info = tarfile.TarInfo(archive_name)
            info.mode = stat.S_IMODE(entry.st_mode)
            info.mtime = int(entry.st_mtime)
            info.uid = entry.st_uid
            info.gid = entry.st_gid
            if stat.S_ISLNK(entry.st_mode):
                info.type = tarfile.SYMTYPE
                info.linkname = os.readlink(leaf, dir_fd=parent_fd)
                archive.addfile(info)
                seen.add(archive_name)
                return
            if stat.S_ISREG(entry.st_mode):
                flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
                descriptor = os.open(leaf, flags, dir_fd=parent_fd)
                try:
                    opened = os.fstat(descriptor)
                    if (opened.st_dev, opened.st_ino) != (entry.st_dev, entry.st_ino):
                        raise RuntimeError("source changed during tar open")
                    info.size = opened.st_size
                    with os.fdopen(descriptor, "rb", closefd=False) as payload:
                        archive.addfile(info, payload)
                finally:
                    os.close(descriptor)
                seen.add(archive_name)
                return
            if stat.S_ISDIR(entry.st_mode):
                info.type = tarfile.DIRTYPE
                archive.addfile(info)
                seen.add(archive_name)
                return
            raise RuntimeError("special files are not transferable")

        for relative in requested:
            parts = valid(relative)
            descriptor = os.dup(root_fd)
            try:
                for part in parts[:-1]:
                    child = open_dir(descriptor, part)
                    os.close(descriptor)
                    descriptor = child
                add(descriptor, parts[-1], relative)
            finally:
                os.close(descriptor)
    os.close(root_fd)
    """
).strip()


REMOTE_EXTRACT_CODE = textwrap.dedent(
    r"""
    import errno
    import os
    import secrets
    import shutil
    import stat
    import sys
    import tarfile
    import tempfile

    def valid(name):
        if not name or name in {".", ".."} or name.startswith("/") or "\\" in name:
            raise ValueError("invalid tar member")
        if any(ord(char) < 32 or ord(char) == 127 for char in name):
            raise ValueError("invalid tar member")
        parts = name.split("/")
        if any(part in {"", ".", ".."} for part in parts):
            raise ValueError("invalid tar member")
        return parts

    def beneath(parent, path):
        return path.startswith(parent + "/")

    def open_parent(root_fd, parts, create):
        descriptor = os.dup(root_fd)
        flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
        try:
            for part in parts:
                try:
                    child = os.open(part, flags, dir_fd=descriptor)
                except FileNotFoundError:
                    if not create:
                        raise
                    os.mkdir(part, 0o700, dir_fd=descriptor)
                    child = os.open(part, flags, dir_fd=descriptor)
                os.close(descriptor)
                descriptor = child
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def remove_at(parent_fd, leaf):
        try:
            entry = os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        if stat.S_ISDIR(entry.st_mode) and not stat.S_ISLNK(entry.st_mode):
            descriptor = os.open(
                leaf,
                os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
            try:
                for child in os.listdir(descriptor):
                    remove_at(descriptor, child)
            finally:
                os.close(descriptor)
            os.rmdir(leaf, dir_fd=parent_fd)
        else:
            os.unlink(leaf, dir_fd=parent_fd)

    def copy_to_parent(source, parent_fd, name):
        entry = os.lstat(source)
        if stat.S_ISLNK(entry.st_mode):
            os.symlink(os.readlink(source), name, dir_fd=parent_fd)
            return
        if stat.S_ISREG(entry.st_mode):
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(name, flags, stat.S_IMODE(entry.st_mode), dir_fd=parent_fd)
            try:
                with open(source, "rb") as input_file:
                    with os.fdopen(descriptor, "wb", closefd=False) as output:
                        shutil.copyfileobj(input_file, output, length=1024 * 1024)
            finally:
                os.close(descriptor)
            os.chmod(
                name,
                stat.S_IMODE(entry.st_mode),
                dir_fd=parent_fd,
                follow_symlinks=False,
            )
            return
        if not stat.S_ISDIR(entry.st_mode):
            raise ValueError("unsupported staged entry")
        os.mkdir(name, stat.S_IMODE(entry.st_mode), dir_fd=parent_fd)
        os.chmod(
            name,
            stat.S_IMODE(entry.st_mode),
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        descriptor = os.open(
            name,
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_fd,
        )
        try:
            for child in os.listdir(source):
                copy_to_parent(os.path.join(source, child), descriptor, child)
        finally:
            os.close(descriptor)

    root = sys.argv[1]
    requested = sys.argv[2:]
    requested_parts = {name: valid(name) for name in requested}
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
    staging = tempfile.mkdtemp(prefix="remote-sandbox-transfer-")
    archive_file = tempfile.TemporaryFile()
    shutil.copyfileobj(sys.stdin.buffer, archive_file)
    archive_file.seek(0)
    try:
        with tarfile.open(fileobj=archive_file, mode="r:") as archive:
            members = archive.getmembers()
            names = []
            symlinks = set()
            for member in members:
                name = member.name.rstrip("/")
                valid(name)
                if name in names:
                    raise ValueError("duplicate tar member")
                if not any(name == path or beneath(path, name) for path in requested):
                    raise ValueError("tar member outside requested paths")
                if member.issym():
                    symlinks.add(name)
                elif not (member.isfile() or member.isdir()):
                    raise ValueError("unsupported tar member")
                names.append(name)
            for name in names:
                if any(beneath(link, name) for link in symlinks):
                    raise ValueError("tar member has symlink parent")
            for member, name in zip(members, names):
                destination = os.path.join(staging, *valid(name))
                os.makedirs(os.path.dirname(destination), mode=0o700, exist_ok=True)
                if member.isdir():
                    os.mkdir(destination, member.mode & 0o777)
                    os.chmod(destination, member.mode & 0o777, follow_symlinks=False)
                elif member.issym():
                    os.symlink(member.linkname, destination)
                else:
                    payload = archive.extractfile(member)
                    if payload is None:
                        raise ValueError("tar file member has no payload")
                    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
                    descriptor = os.open(destination, flags, member.mode & 0o777)
                    try:
                        with os.fdopen(descriptor, "wb", closefd=False) as output:
                            shutil.copyfileobj(payload, output, length=1024 * 1024)
                    finally:
                        payload.close()
                        os.close(descriptor)
                    os.chmod(destination, member.mode & 0o777, follow_symlinks=False)

        top_level = []
        for path in requested:
            if not any(other != path and beneath(other, path) for other in requested):
                top_level.append(path)
        for relative in top_level:
            parts = requested_parts[relative]
            parent_fd = open_parent(root_fd, parts[:-1], True)
            source = os.path.join(staging, *parts)
            temporary = ".remote-sandbox-new-" + secrets.token_hex(8)
            backup = ".remote-sandbox-old-" + secrets.token_hex(8)
            had_destination = False
            try:
                try:
                    os.rename(source, temporary, dst_dir_fd=parent_fd)
                except OSError as exc:
                    if exc.errno != errno.EXDEV:
                        raise
                    copy_to_parent(source, parent_fd, temporary)
                try:
                    os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
                    had_destination = True
                    os.rename(parts[-1], backup, src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
                except FileNotFoundError:
                    pass
                try:
                    os.rename(
                        temporary, parts[-1], src_dir_fd=parent_fd, dst_dir_fd=parent_fd
                    )
                except BaseException:
                    if had_destination:
                        os.rename(backup, parts[-1], src_dir_fd=parent_fd, dst_dir_fd=parent_fd)
                    raise
                if had_destination:
                    remove_at(parent_fd, backup)
            finally:
                os.close(parent_fd)
    finally:
        archive_file.close()
        shutil.rmtree(staging, ignore_errors=True)
        os.close(root_fd)
    """
).strip()
