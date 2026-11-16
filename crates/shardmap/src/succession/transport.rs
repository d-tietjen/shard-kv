//! Bounded private Unix control transfer. No cache operations use this channel.
use super::{Credential, Error, Peer, Readiness};
use std::io::{self, Read, Write};
use std::mem::{size_of, zeroed};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;

pub(super) fn authenticate(stream: &UnixStream, expected: Peer) -> Result<(), Error> {
    // SAFETY: ucred and socklen_t are writable and sized for SO_PEERCRED.
    let mut cred: libc::ucred = unsafe { zeroed() };
    let mut len = size_of::<libc::ucred>() as libc::socklen_t;
    if unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut cred as *mut libc::ucred).cast(),
            &mut len,
        )
    } != 0
    {
        return Err(io::Error::last_os_error().into());
    }
    if len as usize != size_of::<libc::ucred>()
        || cred.pid <= 0
        || expected.pid != cred.pid as u32
        || expected.uid != cred.uid
    {
        return Err(Error::Unauthorized);
    }
    Ok(())
}

pub(super) fn send(
    stream: &mut UnixStream,
    fd: RawFd,
    receipt: &Readiness,
    credential: &[u8],
) -> Result<(), Error> {
    if credential.len() != 32 {
        return Err(Error::Unauthorized);
    }
    let bytes = serde_json::to_vec(receipt).map_err(io::Error::other)?;
    if bytes.len() > 2048 {
        return Err(Error::Profile);
    }
    let mut tag = [1u8];
    let mut iov = libc::iovec {
        iov_base: tag.as_mut_ptr().cast(),
        iov_len: 1,
    };
    // A u64 buffer supplies cmsghdr alignment, unlike a byte array.
    let mut control = [0u64; 8];
    // SAFETY: zeroed msghdr is initialized with valid buffers below.
    let mut msg: libc::msghdr = unsafe { zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    // SAFETY: fixed single-fd payload fits the aligned control buffer; pointers
    // remain valid for the synchronous sendmsg call and ownership stays with A.
    unsafe {
        msg.msg_controllen = libc::CMSG_SPACE(size_of::<RawFd>() as u32) as usize;
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN(size_of::<RawFd>() as u32) as usize;
        libc::CMSG_DATA(cmsg).cast::<RawFd>().write(fd);
        if libc::sendmsg(stream.as_raw_fd(), &msg, libc::MSG_NOSIGNAL) != 1 {
            return Err(io::Error::last_os_error().into());
        }
    }
    stream.write_all(&(bytes.len() as u32).to_le_bytes())?;
    stream.write_all(&bytes)?;
    stream.write_all(credential)?;
    Ok(())
}

pub(super) fn receive(stream: &mut UnixStream) -> Result<(OwnedFd, Readiness, Credential), Error> {
    let mut tag = [0u8];
    let mut iov = libc::iovec {
        iov_base: tag.as_mut_ptr().cast(),
        iov_len: 1,
    };
    let mut control = [0u64; 8];
    // SAFETY: zeroed msghdr initialized with live, aligned receive buffers.
    let mut msg: libc::msghdr = unsafe { zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = size_of_val(&control);
    // SAFETY: all buffers valid for synchronous recvmsg; CLOEXEC is atomic.
    let n = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, libc::MSG_CMSG_CLOEXEC) };
    if n < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let mut descriptors = Vec::new();
    // SAFETY: iterate only kernel-produced cmsgs in the bounded control buffer;
    // each received descriptor is immediately owned, including on error paths.
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let base = libc::CMSG_LEN(0) as usize;
                if (*cmsg).cmsg_len < base {
                    return Err(Error::Profile);
                }
                let count = ((*cmsg).cmsg_len - base) / size_of::<RawFd>();
                for i in 0..count {
                    descriptors.push(OwnedFd::from_raw_fd(
                        libc::CMSG_DATA(cmsg).cast::<RawFd>().add(i).read(),
                    ));
                }
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
    }
    if n != 1
        || tag != [1]
        || msg.msg_flags & (libc::MSG_CTRUNC | libc::MSG_TRUNC) != 0
        || descriptors.len() != 1
    {
        return Err(Error::Profile);
    }
    let fd = descriptors.pop().ok_or(Error::Profile)?;
    let mut len = [0u8; 4];
    stream.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len) as usize;
    if len > 2048 {
        return Err(Error::Profile);
    }
    let mut bytes = vec![0; len];
    stream.read_exact(&mut bytes)?;
    let receipt = serde_json::from_slice(&bytes).map_err(|_| Error::Profile)?;
    let mut credential = Credential::new();
    stream.read_exact(credential.bytes_mut())?;
    Ok((fd, receipt, credential))
}
