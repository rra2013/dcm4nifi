def flowFile = session.get()
if (!flowFile) {
    return
}

flowFile.'Test' = true

REL_SUCCESS << flowFile