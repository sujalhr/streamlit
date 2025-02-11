import streamlit as st

st.title("Welcome")
st.write("Streamlit Setup")

if st.button("Say Hello"):
    st.write("Hello, User!")
