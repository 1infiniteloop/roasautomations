const { initializeApp } = require("firebase/app");
const { getFirestore } = require("firebase/firestore");
const { getFunctions, httpsCallable } = require("firebase/functions");

// firebase config
const firebaseConfig = {
    apiKey: "AIzaSyCt8L38IUUaDBWQeiluCk5VIeqDy3tGP5Q",
    authDomain: "roas-e44b8.firebaseapp.com",
    projectId: "roas-e44b8",
    storageBucket: "roas-e44b8.appspot.com",
    messagingSenderId: "183721452853",
    appId: "1:183721452853:web:2deff81f15a6d3fb4f187c",
};

// initialize firebase
let app = initializeApp(firebaseConfig, {
    ignoreUndefinedProperties: true,
});

// firestore instance
const db = getFirestore();
const functions = getFunctions(app);

exports.db = db;
exports.httpsCallable = httpsCallable;
exports.functions = functions;
