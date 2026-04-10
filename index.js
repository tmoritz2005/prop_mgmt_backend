import { createRouter, createWebHistory } from 'vue-router';
import PropertyList from '../views/PropertyList.vue';
import PropertyDetails from '../views/PropertyDetails.vue';
import PropertyForm from '../views/PropertyForm.vue';

const routes = [
  { path: '/', name: 'Home', component: PropertyList },
  { path: '/property/:id', name: 'PropertyDetails', component: PropertyDetails, props: true },
  { path: '/add-property', name: 'AddProperty', component: PropertyForm },
  { path: '/edit-property/:id', name: 'EditProperty', component: PropertyForm, props: true },
];

const router = createRouter({
  history: createWebHistory(),
  routes,
});

export default router;