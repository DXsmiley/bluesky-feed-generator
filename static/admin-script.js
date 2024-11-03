function toast(string) {
    let toast = document.createElement('div');
    toast.setAttribute('class', 'toast');
    toast.textContent = string;
    let box = document.getElementById('toastbox');
    box.appendChild(toast);
    window.setTimeout((() => box.removeChild(toast)), 10000);
}

function togglestrip(off, on) {
    for (let i of off) {
        document.getElementById(i).classList.remove('selected')
    }
    document.getElementById(on).classList.add('selected')
}

async function post(url, payload) {
    const response = await fetch(
        url,
        {
            method: 'POST',
            cache: 'no-cache',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(payload)
        }
    );
    const text = await response.text();
    toast(`${response.status} ${response.statusText} - ${text}`);
}

async function set_include_in_fox_feed(handle, did, include) {
    togglestrip([`${handle}-ff-false`, `${handle}-ff-null`, `${handle}-ff-true`], `${handle}-ff-${include}`);
    await post('/admin/mark', {did: did, include_in_fox_feed: include});
}

async function set_include_in_vix_feed(handle, did, include) {
    togglestrip([`${handle}-vf-false`, `${handle}-vf-null`, `${handle}-vf-true`], `${handle}-vf-${include}`);
    await post('/admin/mark', {did: did, include_in_vix_feed: include});
}

async function scan_likes(uri) {
    await post('/admin/scan_likes', {uri: uri});
}

async function set_post_pinned(uri, pin) {
    togglestrip([`${uri}-pinned-${!pin}`], `${uri}-pinned-${pin}`);
    await post('/admin/pin_post', {uri: uri, pin: pin});
}

async function cancel_post(id) {
    await post('/schedule/cancel', {id: id});
}

async function post_post_immediately(id) {
    await post('/schedule/post_immediately', {id: id});
}

async function post_rechedule(id) {
    await post('/schedule/rechedule', {id: id});
}